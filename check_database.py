"""
Check database connection and table structure
Supports both BusTrip and Baseline tables
"""
import pyodbc
import logging
import sys
import argparse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def check_database_connection():
    """Check database connection"""
    try:
        # Connection string (Windows authentication)
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=localhost;"
            "DATABASE=BusSim;"
            "Trusted_Connection=yes;"
        )
        
        logger.info("Attempting to connect to SQL Server...")
        conn = pyodbc.connect(conn_str, timeout=10)
        logger.info("Database connection successful")
        
        return conn
        
    except pyodbc.Error as e:
        logger.error(f"Database connection failed: {e}")
        
        # Try connecting to master database to check server
        try:
            master_conn_str = (
                "DRIVER={ODBC Driver 17 for SQL Server};"
                "SERVER=localhost;"
                "DATABASE=master;"
                "Trusted_Connection=yes;"
            )
            master_conn = pyodbc.connect(master_conn_str, timeout=10)
            logger.info("SQL Server is running, but BusSim database may not exist")
            
            # Create database
            cursor = master_conn.cursor()
            cursor.execute("IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'BusSim') CREATE DATABASE BusSim")
            master_conn.commit()
            logger.info("BusSim database has been created")
            
            master_conn.close()
            
            # Retry connection
            return pyodbc.connect(conn_str, timeout=10)
            
        except Exception as e2:
            logger.error(f"SQL Server may not be running or installed: {e2}")
            return None


def check_table_exists(conn, table_name='BusTrip'):
    """Check if table exists"""
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = 'dbo' 
            AND TABLE_NAME = ?
        """, table_name)
        
        count = cursor.fetchone()[0]
        if count > 0:
            logger.info(f"Table dbo.{table_name} exists")
            return True
        else:
            logger.warning(f"Table dbo.{table_name} does not exist")
            return False
            
    except Exception as e:
        logger.error(f"Failed to check table: {e}")
        return False


def get_table_create_sql(table_name='BusTrip'):
    """Get table creation SQL"""
    return f"""
    IF OBJECT_ID('dbo.{table_name}', 'U') IS NOT NULL
        DROP TABLE dbo.{table_name};

    CREATE TABLE dbo.{table_name} (
        -- Primary key
        SIGNID NVARCHAR(100) NOT NULL PRIMARY KEY,
        
        -- Time dimensions
        OPD_DATE DATE NOT NULL,
        WEEKDAY INT NOT NULL CHECK (WEEKDAY BETWEEN 1 AND 7),
        BLOCK NVARCHAR(50) NOT NULL,
        
        -- Route information
        LINEABBR NVARCHAR(20) NOT NULL,
        DIRECTION NVARCHAR(20) NOT NULL,
        TRIP_ID_INT NVARCHAR(100) NOT NULL,
        
        -- Stop information
        STOPABBR NVARCHAR(50),
        SEQUENCE INT,
        
        -- Time metrics
        SCHED_TRIP_TIME DECIMAL(10,2),
        ACT_TRIP_TIME DECIMAL(10,2),
        DIFF_TRIP_TIME DECIMAL(10,2),
        
        -- Distance and speed
        SCHED_DISTANCE DECIMAL(10,2),
        ACT_SPEED DECIMAL(6,2),
        
        -- Vehicle status
        IS_ADDITIONAL BIT NOT NULL DEFAULT 0,
        
        -- Arrival and departure times
        SCHED_ARR_TIME DECIMAL(10,2),
        ACT_ARR_TIME DECIMAL(10,2),
        SCHED_DEP_TIME DECIMAL(10,2),
        ACT_DEP_TIME DECIMAL(10,2),
        DIFF_DEP_TIME DECIMAL(10,2),
        
        -- Stop information
        DWELL_TIME DECIMAL(10,2),
        DISTANCE_TO_NEXT DECIMAL(10,2),
        DISTANCE_TO_TRIP DECIMAL(10,2),
        
        -- Passenger information
        BOARDING INT NOT NULL DEFAULT 0,
        ALIGHTING INT NOT NULL DEFAULT 0,
        LOAD INT NOT NULL DEFAULT 0,
        WHEELCHAIR_COUNT INT NOT NULL DEFAULT 0,
        
        -- Audit fields
        CREATED_AT DATETIME2 DEFAULT GETDATE(),
        UPDATED_AT DATETIME2 DEFAULT GETDATE()
    );
    """


def get_index_create_sql(table_name='BusTrip'):
    """Get index creation SQL"""
    return f"""
    CREATE NONCLUSTERED INDEX IX_{table_name}_OPD_DATE ON dbo.{table_name}(OPD_DATE);
    CREATE NONCLUSTERED INDEX IX_{table_name}_LINEABBR ON dbo.{table_name}(LINEABBR);
    CREATE NONCLUSTERED INDEX IX_{table_name}_TRIP_ID_INT ON dbo.{table_name}(TRIP_ID_INT);
    CREATE NONCLUSTERED INDEX IX_{table_name}_BLOCK ON dbo.{table_name}(BLOCK);
    CREATE NONCLUSTERED INDEX IX_{table_name}_Line_Date ON dbo.{table_name}(LINEABBR, OPD_DATE) 
        INCLUDE (BOARDING, ALIGHTING, LOAD);
    """


def get_trigger_create_sql(table_name='BusTrip'):
    """Get trigger creation SQL"""
    return f"""
    CREATE TRIGGER trg_{table_name}_UpdateTimestamp
    ON dbo.{table_name}
    AFTER UPDATE
    AS
    BEGIN
        SET NOCOUNT ON;
        UPDATE dbo.{table_name}
        SET UPDATED_AT = GETDATE()
        FROM dbo.{table_name} t
        INNER JOIN inserted i ON t.SIGNID = i.SIGNID;
    END;
    """


def create_table(conn, table_name='BusTrip'):
    """Create table"""
    try:
        cursor = conn.cursor()
        
        # Read and execute table creation SQL
        logger.info(f"Attempting to create table {table_name}...")
        
        # Create table
        create_sql = get_table_create_sql(table_name)
        cursor.execute(create_sql)
        conn.commit()
        logger.info(f"Table {table_name} created successfully")
        
        # Create indexes
        index_sql = get_index_create_sql(table_name)
        cursor.execute(index_sql)
        conn.commit()
        logger.info(f"Indexes for table {table_name} created successfully")
        
        # Create trigger
        trigger_sql = get_trigger_create_sql(table_name)
        cursor.execute(trigger_sql)
        conn.commit()
        logger.info(f"Trigger for table {table_name} created successfully")
        
        return True
        
    except Exception as e:
        logger.error(f"Failed to create table {table_name}: {e}")
        return False


def check_table_data(conn, table_name='BusTrip'):
    """Check data in table"""
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name}")
        count = cursor.fetchone()[0]
        
        if count > 0:
            logger.info(f"Table {table_name} contains {count} records")
            
            # Show some sample data
            cursor.execute(f"SELECT TOP 5 SIGNID, LINEABBR, OPD_DATE, LOAD FROM dbo.{table_name} ORDER BY CREATED_AT DESC")
            rows = cursor.fetchall()
            
            logger.info(f"Latest 5 records in table {table_name}:")
            for row in rows:
                logger.info(f"  - {row[0]}: Route {row[1]}, Date {row[2]}, Load {row[3]} passengers")
        else:
            logger.warning(f"Table {table_name} contains no data")
            
        return count
        
    except Exception as e:
        logger.error(f"Failed to query table {table_name} data: {e}")
        return 0


def check_and_create_table(conn, table_name):
    """Check and create single table"""
    logger.info(f"\nChecking table {table_name}...")
    logger.info("-" * 40)
    
    if not check_table_exists(conn, table_name):
        # Try to create table
        if create_table(conn, table_name):
            logger.info(f"Table {table_name} created successfully")
        else:
            logger.error(f"Failed to create table {table_name}")
            return False
    
    # Check data in table
    data_count = check_table_data(conn, table_name)
    
    return True


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Check and create database tables')
    parser.add_argument('--table', choices=['BusTrip', 'Baseline', 'both'], 
                      default='both', help='Table to check (default: both)')
    parser.add_argument('--create-only', action='store_true', 
                      help='Only create tables, do not check data')
    args = parser.parse_args()
    
    logger.info("="*60)
    logger.info("Database Check and Creation Tool")
    logger.info("="*60)
    
    # 1. Check database connection
    conn = check_database_connection()
    if not conn:
        logger.error("Unable to connect to database, please check if SQL Server is running")
        return 1
    
    # 2. Determine tables to process based on parameters
    tables_to_check = []
    if args.table == 'both':
        tables_to_check = ['BusTrip', 'Baseline']
    else:
        tables_to_check = [args.table]
    
    # 3. Check and create tables
    all_success = True
    for table_name in tables_to_check:
        if not check_and_create_table(conn, table_name):
            all_success = False
    
    # 4. Close connection
    conn.close()
    
    logger.info("\n" + "="*60)
    if all_success:
        logger.info("All specified tables are ready")
        if not args.create_only:
            logger.info("Simulation can now be run")
    else:
        logger.info("Some tables failed to create, please check error messages")
    logger.info("="*60)
    
    return 0 if all_success else 1


if __name__ == "__main__":
    sys.exit(main()) 