"""
检查数据库连接和表结构
支持BusTrip和Baseline两个表
"""
import pyodbc
import logging
import sys
import argparse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def check_database_connection():
    """检查数据库连接"""
    try:
        # 连接字符串（Windows认证）
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=localhost;"
            "DATABASE=BusSim;"
            "Trusted_Connection=yes;"
        )
        
        logger.info("尝试连接到SQL Server...")
        conn = pyodbc.connect(conn_str, timeout=10)
        logger.info("✓ 数据库连接成功")
        
        return conn
        
    except pyodbc.Error as e:
        logger.error(f"✗ 数据库连接失败: {e}")
        
        # 尝试连接到master数据库检查服务器
        try:
            master_conn_str = (
                "DRIVER={ODBC Driver 17 for SQL Server};"
                "SERVER=localhost;"
                "DATABASE=master;"
                "Trusted_Connection=yes;"
            )
            master_conn = pyodbc.connect(master_conn_str, timeout=10)
            logger.info("✓ SQL Server正在运行，但BusSim数据库可能不存在")
            
            # 创建数据库
            cursor = master_conn.cursor()
            cursor.execute("IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'BusSim') CREATE DATABASE BusSim")
            master_conn.commit()
            logger.info("✓ 已创建BusSim数据库")
            
            master_conn.close()
            
            # 重新尝试连接
            return pyodbc.connect(conn_str, timeout=10)
            
        except Exception as e2:
            logger.error(f"✗ SQL Server可能未运行或未安装: {e2}")
            return None


def check_table_exists(conn, table_name='BusTrip'):
    """检查表是否存在"""
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
            logger.info(f"✓ 表dbo.{table_name}存在")
            return True
        else:
            logger.warning(f"✗ 表dbo.{table_name}不存在")
            return False
            
    except Exception as e:
        logger.error(f"检查表失败: {e}")
        return False


def get_table_create_sql(table_name='BusTrip'):
    """获取建表SQL"""
    return f"""
    IF OBJECT_ID('dbo.{table_name}', 'U') IS NOT NULL
        DROP TABLE dbo.{table_name};

    CREATE TABLE dbo.{table_name} (
        -- 主键
        SIGNID NVARCHAR(100) NOT NULL PRIMARY KEY,
        
        -- 时间维度
        OPD_DATE DATE NOT NULL,
        WEEKDAY INT NOT NULL CHECK (WEEKDAY BETWEEN 1 AND 7),
        BLOCK NVARCHAR(50) NOT NULL,
        
        -- 线路信息
        LINEABBR NVARCHAR(20) NOT NULL,
        DIRECTION NVARCHAR(20) NOT NULL,
        TRIP_ID_INT NVARCHAR(100) NOT NULL,
        
        -- 站点信息
        STOPABBR NVARCHAR(50),
        SEQUENCE INT,
        
        -- 时间指标
        SCHED_TRIP_TIME DECIMAL(10,2),
        ACT_TRIP_TIME DECIMAL(10,2),
        DIFF_TRIP_TIME DECIMAL(10,2),
        
        -- 距离和速度
        SCHED_DISTANCE DECIMAL(10,2),
        ACT_SPEED DECIMAL(6,2),
        
        -- 车辆状态
        IS_ADDITIONAL BIT NOT NULL DEFAULT 0,
        
        -- 到站和离站时间
        SCHED_ARR_TIME DECIMAL(10,2),
        ACT_ARR_TIME DECIMAL(10,2),
        SCHED_DEP_TIME DECIMAL(10,2),
        ACT_DEP_TIME DECIMAL(10,2),
        DIFF_DEP_TIME DECIMAL(10,2),
        
        -- 停站信息
        DWELL_TIME DECIMAL(10,2),
        DISTANCE_TO_NEXT DECIMAL(10,2),
        DISTANCE_TO_TRIP DECIMAL(10,2),
        
        -- 乘客信息
        BOARDING INT NOT NULL DEFAULT 0,
        ALIGHTING INT NOT NULL DEFAULT 0,
        LOAD INT NOT NULL DEFAULT 0,
        WHEELCHAIR_COUNT INT NOT NULL DEFAULT 0,
        
        -- 审计字段
        CREATED_AT DATETIME2 DEFAULT GETDATE(),
        UPDATED_AT DATETIME2 DEFAULT GETDATE()
    );
    """


def get_index_create_sql(table_name='BusTrip'):
    """获取创建索引的SQL"""
    return f"""
    CREATE NONCLUSTERED INDEX IX_{table_name}_OPD_DATE ON dbo.{table_name}(OPD_DATE);
    CREATE NONCLUSTERED INDEX IX_{table_name}_LINEABBR ON dbo.{table_name}(LINEABBR);
    CREATE NONCLUSTERED INDEX IX_{table_name}_TRIP_ID_INT ON dbo.{table_name}(TRIP_ID_INT);
    CREATE NONCLUSTERED INDEX IX_{table_name}_BLOCK ON dbo.{table_name}(BLOCK);
    CREATE NONCLUSTERED INDEX IX_{table_name}_Line_Date ON dbo.{table_name}(LINEABBR, OPD_DATE) 
        INCLUDE (BOARDING, ALIGHTING, LOAD);
    """


def get_trigger_create_sql(table_name='BusTrip'):
    """获取创建触发器的SQL"""
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
    """创建表"""
    try:
        cursor = conn.cursor()
        
        # 读取并执行建表SQL
        logger.info(f"尝试创建表{table_name}...")
        
        # 创建表
        create_sql = get_table_create_sql(table_name)
        cursor.execute(create_sql)
        conn.commit()
        logger.info(f"✓ 表{table_name}创建成功")
        
        # 创建索引
        index_sql = get_index_create_sql(table_name)
        cursor.execute(index_sql)
        conn.commit()
        logger.info(f"✓ 表{table_name}的索引创建成功")
        
        # 创建触发器
        trigger_sql = get_trigger_create_sql(table_name)
        cursor.execute(trigger_sql)
        conn.commit()
        logger.info(f"✓ 表{table_name}的触发器创建成功")
        
        return True
        
    except Exception as e:
        logger.error(f"创建表{table_name}失败: {e}")
        return False


def check_table_data(conn, table_name='BusTrip'):
    """检查表中的数据"""
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM dbo.{table_name}")
        count = cursor.fetchone()[0]
        
        if count > 0:
            logger.info(f"✓ 表{table_name}中有{count}条记录")
            
            # 显示一些样本数据
            cursor.execute(f"SELECT TOP 5 SIGNID, LINEABBR, OPD_DATE, LOAD FROM dbo.{table_name} ORDER BY CREATED_AT DESC")
            rows = cursor.fetchall()
            
            logger.info(f"表{table_name}最新的5条记录:")
            for row in rows:
                logger.info(f"  - {row[0]}: 线路{row[1]}, 日期{row[2]}, 载客{row[3]}人")
        else:
            logger.warning(f"✗ 表{table_name}中没有数据")
            
        return count
        
    except Exception as e:
        logger.error(f"查询表{table_name}数据失败: {e}")
        return 0


def check_and_create_table(conn, table_name):
    """检查并创建单个表"""
    logger.info(f"\n检查表{table_name}...")
    logger.info("-" * 40)
    
    if not check_table_exists(conn, table_name):
        # 尝试创建表
        if create_table(conn, table_name):
            logger.info(f"表{table_name}创建成功")
        else:
            logger.error(f"表{table_name}创建失败")
            return False
    
    # 检查表中的数据
    data_count = check_table_data(conn, table_name)
    
    return True


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='检查和创建数据库表')
    parser.add_argument('--table', choices=['BusTrip', 'Baseline', 'both'], 
                      default='both', help='要检查的表（默认：both）')
    parser.add_argument('--create-only', action='store_true', 
                      help='仅创建表，不检查数据')
    args = parser.parse_args()
    
    logger.info("="*60)
    logger.info("数据库检查和创建工具")
    logger.info("="*60)
    
    # 1. 检查数据库连接
    conn = check_database_connection()
    if not conn:
        logger.error("无法连接到数据库，请检查SQL Server是否正在运行")
        return 1
    
    # 2. 根据参数决定要处理的表
    tables_to_check = []
    if args.table == 'both':
        tables_to_check = ['BusTrip', 'Baseline']
    else:
        tables_to_check = [args.table]
    
    # 3. 检查并创建表
    all_success = True
    for table_name in tables_to_check:
        if not check_and_create_table(conn, table_name):
            all_success = False
    
    # 4. 关闭连接
    conn.close()
    
    logger.info("\n" + "="*60)
    if all_success:
        logger.info("✓ 所有指定的表都已就绪")
        if not args.create_only:
            logger.info("可以运行仿真程序了")
    else:
        logger.info("✗ 有些表创建失败，请检查错误信息")
    logger.info("="*60)
    
    return 0 if all_success else 1


if __name__ == "__main__":
    sys.exit(main()) 