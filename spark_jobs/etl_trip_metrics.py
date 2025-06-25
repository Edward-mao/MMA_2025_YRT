"""
PySpark ETL job - Bus trip metrics processing
From SimPy event files, perform cleaning, transformation, and aggregation, and write to SQL Server
"""
import os
import sys
from datetime import datetime
from typing import Optional, List, Dict, Any
import logging
import pyodbc
import time

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, when, coalesce, round as spark_round, 
    abs as spark_abs, udf, from_json, to_timestamp,
    date_format, dayofweek, hour, mean, stddev, count,
    max as spark_max, min as spark_min, sum as spark_sum,
    from_unixtime
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, BooleanType, TimestampType
)
# from pyspark.sql import Window  # Not used, commented out for now
import yaml

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BusTripETL:
    """ETL processing class for bus trip data."""
    
    def __init__(self, config_path: str = "config/db.yml", reset_database: bool = True, data_target: str = "scenario"):
        """
        Initializes the ETL processor.
        
        Args:
            config_path: The path to the configuration file.
            reset_database: Whether to reset the database table (defaults to True).
            data_target: The data write target ("scenario" or "baseline").
        """
        self.config = self._load_config(config_path)
        self.spark = self._create_spark_session()
        self.data_target = data_target
        
        # Generate a unique run_id for this run
        self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Determine the target table based on data_target
        db_cfg = self.config.get('database', {})
        if self.data_target == "baseline":
            self.target_table = "dbo.Baseline"
        else:
            self.target_table = db_cfg.get('table', 'dbo.BusTrip')
        
        logger.info(f"Data will be written to table: {self.target_table}")
        
        # Only clear old data from the table if necessary
        if reset_database:
            try:
                self._truncate_target_table(db_cfg)
                logger.info(f"Database table {self.target_table} has been cleared")
            except Exception as e:
                logger.warning(f"Could not clear table {self.target_table}: {e}")
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Loads the configuration file."""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.warning(f"Could not load config file {config_path}: {e}")
            # Return default configuration
            return {
                'database': {
                    'server': 'localhost',
                    'database': 'BusSim',
                    'table': 'dbo.BusTrip',
                    'batch_size': 10000,
                    'connection_timeout': 30
                },
                'spark': {
                    'app_name': 'BusTripETL',
                    'master': 'local[*]',
                    'shuffle_partitions': 200,
                    'adaptive_enabled': True
                }
            }
            
    def _find_jdbc_driver(self) -> Optional[str]:
        """Finds the JDBC driver file."""
        # First check the environment variable
        if os.environ.get('MSSQL_JDBC_DRIVER'):
            driver_path = os.environ.get('MSSQL_JDBC_DRIVER')
            if os.path.exists(driver_path):
                return driver_path
        
        # Possible driver file names
        driver_names = [
            "mssql-jdbc-11.2.0.jre8.jar",
            "mssql-jdbc-11.2.0.jre11.jar",
            "mssql-jdbc-12.2.0.jre8.jar", 
            "mssql-jdbc-12.2.0.jre11.jar",
            "sqljdbc42.jar",
            "sqljdbc41.jar"
        ]
        
        # Check several possible locations
        search_paths = [
            ".",  # Project root directory
            "spark/jar",  # Location mentioned by the user
            "spark/jars",  # Standard Spark location
            os.path.join(os.environ.get("SPARK_HOME", ""), "jars") if os.environ.get("SPARK_HOME") else None
        ]
        
        for path in search_paths:
            if path and os.path.exists(path):
                # Check specific driver file names
                for driver in driver_names:
                    driver_path = os.path.join(path, driver)
                    if os.path.exists(driver_path):
                        return driver_path
                
                # Check any mssql or sqljdbc drivers
                if os.path.isdir(path):
                    for file in os.listdir(path):
                        if (file.startswith("mssql-jdbc") or file.startswith("sqljdbc")) and file.endswith(".jar"):
                            driver_path = os.path.join(path, file)
                            return driver_path
        
        logger.warning("JDBC driver file not found, trying to use default configuration")
        return None
            
    def _create_spark_session(self) -> SparkSession:
        """Creates a Spark session."""
        spark_config = self.config.get('spark', {})
        
        builder = SparkSession.builder \
            .appName(spark_config.get('app_name', 'BusTripETL')) \
            .master(spark_config.get('master', 'local[*]'))
            
        # Set Spark configurations
        builder = builder \
            .config("spark.sql.shuffle.partitions", 
                   spark_config.get('shuffle_partitions', 100)) \
            .config("spark.sql.adaptive.enabled", 
                   str(spark_config.get('adaptive_enabled', True)).lower()) \
            .config("spark.sql.adaptive.coalescePartitions.enabled", 
                   str(spark_config.get('sql_adaptive_coalesce_partitions', True)).lower()) \
            .config("spark.sql.adaptive.skewJoin.enabled", 
                   str(spark_config.get('sql_adaptive_skew_join', True)).lower()) \
            .config("spark.driver.memory", spark_config.get('driver_memory', '6g')) \
            .config("spark.executor.memory", spark_config.get('executor_memory', '6g')) \
            .config("spark.driver.maxResultSize", "2g") \
            .config("spark.sql.streaming.checkpointLocation.deleteTmpCheckpointDir", "true") \
            .config("spark.sql.debug.maxToStringFields", 
                   str(spark_config.get('sql_debug_maxToStringFields', 100))) \
            .config("spark.sql.planChangeLog.level", 
                   spark_config.get('sql_planChangeLog_level', 'WARN')) \
            .config("spark.sql.planChangeLog.batches", 
                   str(spark_config.get('sql_planChangeLog_batches', 5)))
        # New: Lengthen heartbeat and network timeout to avoid heartbeat loss due to large batch tasks or GC blocking
        builder = builder.config("spark.executor.heartbeatInterval", spark_config.get('executor_heartbeat_interval', '30s')) \
                         .config("spark.network.timeout", spark_config.get('network_timeout', '600s'))
            
        # Add serializer configuration
        if spark_config.get('serializer'):
            builder = builder.config("spark.serializer", spark_config.get('serializer'))
            
        # Add adaptive query execution configuration
        if spark_config.get('sql_adaptive_enabled'):
            builder = builder.config("spark.sql.adaptive.enabled", "true")
        if spark_config.get('sql_adaptive_localShuffleReader_enabled'):
            builder = builder.config("spark.sql.adaptive.localShuffleReader.enabled", "true")
            
        # JDBC driver configuration
        jdbc_driver_path = self._find_jdbc_driver()
        if jdbc_driver_path:
            builder = builder.config("spark.jars", jdbc_driver_path)
            logger.info(f"Using JDBC driver: {jdbc_driver_path}")
            
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        
        logger.info(f"Spark session created - Driver memory: {spark_config.get('driver_memory', '6g')}, "
                   f"Executor memory: {spark_config.get('executor_memory', '6g')}")
        return spark
        
    def get_event_schema(self) -> StructType:
        """Gets the schema for the event data."""
        return StructType([
            StructField("sign_id", StringType(), False),
            StructField("opd_date", StringType(), True),
            StructField("weekday", IntegerType(), True),
            StructField("block", StringType(), True),
            StructField("line_abbr", StringType(), True),
            StructField("direction", StringType(), True),
            StructField("trip_id_int", StringType(), True),
            StructField("sched_arr_time", DoubleType(), True),
            StructField("act_arr_time", DoubleType(), True),
            StructField("sched_dep_time", DoubleType(), True),
            StructField("act_dep_time", DoubleType(), True),
            StructField("dwell_time", DoubleType(), True),
            StructField("sched_trip_time", DoubleType(), True),
            StructField("act_trip_time", DoubleType(), True),
            StructField("diff_trip_time", DoubleType(), True),
            StructField("diff_dep_time", DoubleType(), True),
            StructField("stop_id", StringType(), True),
            StructField("stop_sequence", IntegerType(), True),
            StructField("sched_distance", DoubleType(), True),
            StructField("act_speed", DoubleType(), True),
            StructField("distance_to_next", DoubleType(), True),
            StructField("distance_to_trip", DoubleType(), True),
            StructField("boarding", IntegerType(), True),
            StructField("alighting", IntegerType(), True),
            StructField("load", IntegerType(), True),
            StructField("wheelchair_count", IntegerType(), True),
            StructField("is_additional", BooleanType(), True),
            StructField("event_type", StringType(), True),
            StructField("timestamp", DoubleType(), True)
        ])
        
    def read_events(self, input_path: str) -> DataFrame:
        """
        Reads event data files.
        
        Args:
            input_path: The input file path (supports wildcards).
            
        Returns:
            DataFrame: The event data.
        """
        schema = self.get_event_schema()
        
        # Read JSON Lines files
        df = self.spark.read \
            .option("multiLine", False) \
            .schema(schema) \
            .json(input_path)
            
        logger.info(f"Read {df.count()} event records")
        return df
        
    def transform_events(self, df: DataFrame) -> DataFrame:
        """
        Transforms event data.
        """
        # Keep only arrival events
        arrival_df = df.filter(col("event_type") == "arrival")
        
        # In streaming processing, a watermark needs to be added
        if arrival_df.isStreaming:
            # Convert timestamp from seconds to a timestamp type
            # Assume timestamp is the number of seconds since 1970-01-01 00:00:00
            arrival_df = arrival_df.withColumn(
                "event_time", 
                to_timestamp(from_unixtime(col("timestamp")))
            )
            # Use the event_time field as the event time and set a 10-minute watermark
            arrival_df = arrival_df.withWatermark("event_time", "10 minutes")

        # Calculate the first/last scheduled/actual arrival time for each trip for trip time calculation
        trip_metrics_df = arrival_df.groupBy("trip_id_int").agg(
            spark_min("sched_arr_time").alias("sched_first"),
            spark_max("sched_arr_time").alias("sched_last"),
            spark_min("act_arr_time").alias("act_first"),
            spark_max("act_arr_time").alias("act_last"),
            spark_max("sched_distance").alias("trip_sched_distance")
        ).withColumn(
            "SCHED_TRIP_TIME_CALC", col("sched_last") - col("sched_first")
        ).withColumn(
            "ACT_TRIP_TIME_CALC", col("act_last") - col("act_first")
        ).withColumn(
            "DIFF_TRIP_TIME_CALC", col("act_last") - col("sched_last")
        ).withColumn(
            "ACT_SPEED_CALC",
            when(col("ACT_TRIP_TIME_CALC") > 0, (col("trip_sched_distance") * 3.6) / col("ACT_TRIP_TIME_CALC"))
            .otherwise(None)
        )

        # Join trip metrics back to arrival records
        enriched_df = arrival_df.join(trip_metrics_df.select(
            "trip_id_int", "SCHED_TRIP_TIME_CALC", "ACT_TRIP_TIME_CALC",
            "DIFF_TRIP_TIME_CALC", "ACT_SPEED_CALC", "trip_sched_distance"
        ), on="trip_id_int", how="left")

        # Map to SQL Server table fields
        transformed_df = enriched_df.select(
            col("sign_id").alias("SIGNID"),
            col("opd_date").alias("OPD_DATE"),
            col("weekday").alias("WEEKDAY"),
            col("block").alias("BLOCK"),
            col("line_abbr").alias("LINEABBR"),
            col("direction").alias("DIRECTION"),
            col("trip_id_int").alias("TRIP_ID_INT"),
            col("stop_id").alias("STOPABBR"),
            col("stop_sequence").alias("SEQUENCE"),
            spark_round(col("SCHED_TRIP_TIME_CALC"), 2).alias("SCHED_TRIP_TIME"),
            spark_round(col("ACT_TRIP_TIME_CALC"), 2).alias("ACT_TRIP_TIME"),
            spark_round(col("DIFF_TRIP_TIME_CALC"), 2).alias("DIFF_TRIP_TIME"),
            spark_round(col("trip_sched_distance"), 2).alias("SCHED_DISTANCE"),
            spark_round(col("ACT_SPEED_CALC"), 2).alias("ACT_SPEED"),
            col("is_additional").alias("IS_ADDITIONAL"),
            spark_round(col("sched_arr_time"), 2).alias("SCHED_ARR_TIME"),
            spark_round(col("act_arr_time"), 2).alias("ACT_ARR_TIME"),
            spark_round(col("sched_dep_time"), 2).alias("SCHED_DEP_TIME"),
            spark_round(col("act_dep_time"), 2).alias("ACT_DEP_TIME"),
            spark_round(col("diff_dep_time"), 2).alias("DIFF_DEP_TIME"),
            spark_round(col("dwell_time"), 2).alias("DWELL_TIME"),
            spark_round(col("distance_to_next"), 2).alias("DISTANCE_TO_NEXT"),
            spark_round(col("distance_to_trip"), 2).alias("DISTANCE_TO_TRIP"),
            col("boarding").alias("BOARDING"),
            col("alighting").alias("ALIGHTING"),
            col("load").alias("LOAD"),
            col("wheelchair_count").alias("WHEELCHAIR_COUNT")
        )

        # Apply data quality checks
        transformed_df = self._apply_quality_checks(transformed_df)

        # Deduplicate
        if not transformed_df.isStreaming:
            transformed_df = transformed_df.dropDuplicates(["SIGNID"])

        # Logging
        if transformed_df.isStreaming:
            logger.info("Got streaming DataFrame after transformation, skipping count")
        else:
            logger.info(f"{transformed_df.count()} records remaining after transformation")

        return transformed_df
        
    def _apply_quality_checks(self, df: DataFrame) -> DataFrame:
        """Applies data quality checks and corrections."""
        # Correct negative values
        numeric_columns = [
            "SCHED_TRIP_TIME", "ACT_TRIP_TIME", 
            "SCHED_DISTANCE", "ACT_SPEED",
            "DISTANCE_TO_NEXT", "DISTANCE_TO_TRIP",
            "DWELL_TIME", "BOARDING", "ALIGHTING", 
            "LOAD", "WHEELCHAIR_COUNT"
        ]
        
        for col_name in numeric_columns:
            df = df.withColumn(
                col_name,
                when(col(col_name) < 0, 0).otherwise(col(col_name))
            )
            
        # Correct abnormal speeds (> 120 km/h)
        df = df.withColumn(
            "ACT_SPEED",
            when(col("ACT_SPEED") > 120, None).otherwise(col("ACT_SPEED"))
        )
        
        # Ensure wheelchair count does not exceed total load
        df = df.withColumn(
            "WHEELCHAIR_COUNT",
            when(col("WHEELCHAIR_COUNT") > col("LOAD"), col("LOAD"))
                .otherwise(col("WHEELCHAIR_COUNT"))
        )
        
        return df
        
    def write_to_sqlserver(self, df: DataFrame, mode: str = "append") -> None:
        """
        Writes data to SQL Server.
        
        Args:
            df: The DataFrame to write.
            mode: The write mode ("append", "overwrite").
        """
        db_config = self.config.get('database', {})
        
        # JDBC connection URL (using Windows Authentication)
        jdbc_url = (
            f"jdbc:sqlserver://{db_config.get('server', 'localhost')};"
            f"databaseName={db_config.get('database', 'BusSim')};"
            f"integratedSecurity=true;"
            f"encrypt=false;"
            f"trustServerCertificate=true"
        )
        
        # Connection properties
        connection_properties = {
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "batchsize": str(db_config.get('batch_size', 10000)),
            "isolationLevel": "READ_COMMITTED",
            "loginTimeout": str(db_config.get('connection_timeout', 30))
        }
        
        try:
            # Write data
            df.write \
                .mode(mode) \
                .option("truncate", "false") \
                .jdbc(
                    url=jdbc_url,
                    table=self.target_table,
                    properties=connection_properties
                )
                
            logger.info(f"Successfully wrote {df.count()} records to SQL Server table {self.target_table}")
            
        except Exception as e:
            logger.error(f"Failed to write to SQL Server using JDBC: {e}")
            logger.info("Trying to write using pyodbc fallback...")
            try:
                self._write_to_sqlserver_pyodbc(df)
                logger.info("Successfully wrote to the database using pyodbc")
            except Exception as inner_e:
                logger.error(f"pyodbc fallback write also failed: {inner_e}")
                raise
        
    def _write_to_sqlserver_pyodbc(self, df: DataFrame) -> None:
        """Writes a Spark DataFrame to SQL Server using pyodbc (as a fallback for JDBC failure)."""
        # Delay import of pandas to avoid errors in Spark executors
        import pandas as pd
        db_config = self.config.get('database', {})
        driver_name = db_config.get('driver', '{ODBC Driver 17 for SQL Server}')
        server = db_config.get('server', 'localhost')
        database = db_config.get('database', 'BusSim')
        username = db_config.get('username')
        password = db_config.get('password')
        
        if username and password:
            conn_str = (
                f"DRIVER={driver_name};"
                f"SERVER={server};DATABASE={database};"
                f"UID={username};PWD={password};"
                f"TrustServerCertificate=yes;"
            )
        else:
            # Windows authentication
            conn_str = (
                f"DRIVER={driver_name};SERVER={server};DATABASE={database};"
                f"Trusted_Connection=yes;TrustServerCertificate=yes;"
            )
        
        # Collect Spark DataFrame to Pandas (suitable for currently small data volumes)
        pandas_df = df.toPandas()
        if pandas_df.empty:
            logger.warning("Data to be written is empty, skipping pyodbc write")
            return
        
        # Construct the INSERT statement
        columns = list(pandas_df.columns)
        col_list = ", ".join(f"[{c}]" for c in columns)
        placeholders = ", ".join("?" for _ in columns)
        insert_sql = f"INSERT INTO {self.target_table} ({col_list}) VALUES ({placeholders})"
        
        try:
            with pyodbc.connect(conn_str, autocommit=False) as conn:
                cursor = conn.cursor()
                cursor.fast_executemany = True
                cursor.executemany(insert_sql, pandas_df.values.tolist())
                conn.commit()
        except Exception as e:
            logger.error(f"pyodbc write to database failed: {e}")
            raise
        
    def run_batch(self, input_path: str, mode: str = "append") -> None:
        """
        Runs ETL in batch mode.
        
        Args:
            input_path: The input file path (supports wildcards).
            mode: The write mode.
        """
        try:
            # Read data
            events_df = self.read_events(input_path)
            
            # Transform data
            transformed_df = self.transform_events(events_df)
            
            # Write to database
            self.write_to_sqlserver(transformed_df, mode)
            
            # Generate statistics
            stats = self.generate_statistics(transformed_df)
            logger.info(f"ETL statistics: {stats}")
            
        except Exception as e:
            logger.error(f"Batch ETL failed: {e}", exc_info=True)
            raise
            
    def run_streaming(self, input_dir: str) -> None:
        """
        Runs ETL in streaming mode.
        
        Args:
            input_dir: The input directory path.
        """
        schema = self.get_event_schema()
        
        # Streaming read
        stream_df = self.spark.readStream \
            .option("multiLine", False) \
            .schema(schema) \
            .json(input_dir)
            
        # Transform data
        transformed_stream = self.transform_events(stream_df)
        
        # Get the checkpoint directory
        checkpoint_dir = self.config.get('etl', {}).get('checkpoint_dir', './checkpoint')
        
        # Batch processing function
        def write_batch(batch_df: DataFrame, batch_id: int) -> None:
            if batch_df.count() > 0:
                logger.info(f"Processing batch {batch_id} with {batch_df.count()} records")
                self.write_to_sqlserver(batch_df, mode="append")
            else:
                logger.info(f"Batch {batch_id} is empty, skipping")
        
        # Start the streaming query
        query = transformed_stream.writeStream \
            .outputMode("append") \
            .foreachBatch(write_batch) \
            .option("checkpointLocation", checkpoint_dir) \
            .trigger(processingTime="10 seconds") \
            .option("maxFilesPerTrigger", "100") \
            .start()
            
        logger.info(f"Streaming ETL started, listening to directory: {input_dir}")
        
        # Set the query to non-blocking mode
        # Let the main thread continue execution without waiting for the streaming query to finish
        return query
        
    def generate_statistics(self, df: DataFrame) -> Dict[str, Any]:
        """Generates data statistics."""
        stats = {
            "total_records": df.count(),
            "unique_trips": df.select("TRIP_ID_INT").distinct().count(),
            "unique_lines": df.select("LINEABBR").distinct().count(),
            "avg_delay": df.agg(mean("DIFF_DEP_TIME")).collect()[0][0],
            "avg_speed": df.agg(mean("ACT_SPEED")).collect()[0][0],
            "total_boardings": df.agg(spark_sum("BOARDING")).collect()[0][0],
            "total_alightings": df.agg(spark_sum("ALIGHTING")).collect()[0][0]
        }
        return stats
        
    def close(self):
        """Closes the Spark session."""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session closed")
            
    def _truncate_target_table(self, db_cfg: dict):
        """Clears the data from the target table."""
        try:
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={db_cfg.get('server', 'localhost')};"
                f"DATABASE={db_cfg.get('database', 'BusSim')};"
                f"Trusted_Connection=yes;"
            )
            
            with pyodbc.connect(conn_str) as conn:
                cursor = conn.cursor()
                cursor.execute(f"TRUNCATE TABLE {self.target_table}")
                conn.commit()
                logger.info(f"Table {self.target_table} has been cleared")
                
        except Exception as e:
            logger.error(f"Failed to clear table: {e}")
            raise


def main():
    """Main program entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Bus trip data ETL processing")
    parser.add_argument("--input", required=True, help="Input file path or directory")
    parser.add_argument("--mode", choices=["batch", "streaming"], default="batch", 
                      help="Processing mode: batch or streaming")
    parser.add_argument("--config", default="config/db.yml", help="Configuration file path")
    parser.add_argument("--write-mode", choices=["append", "overwrite"], default="append",
                      help="Data write mode")
    parser.add_argument("--data-target", choices=["scenario", "baseline"], default="scenario",
                      help="Target table for data writing")
    args = parser.parse_args()
    
    # Create an ETL instance
    etl = BusTripETL(config_path=args.config, data_target=args.data_target)
    
    try:
        if args.mode == "batch":
            # Batch mode
            etl.run_batch(args.input, mode=args.write_mode)
        else:
            # Streaming mode
            query = etl.run_streaming(args.input)
            query.awaitTermination()
            
    finally:
        etl.close()


if __name__ == "__main__":
    main() 