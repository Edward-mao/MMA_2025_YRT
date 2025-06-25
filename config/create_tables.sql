-- 公交行程数据表创建脚本
-- 适用于SQL Server 2016+

-- 创建数据库（如果不存在）
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'BusSim')
BEGIN
    CREATE DATABASE BusSim
    COLLATE Chinese_PRC_CI_AS;
END
GO

USE BusSim;
GO

-- 创建架构（如果不存在）
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'dbo')
BEGIN
    EXEC('CREATE SCHEMA dbo');
END
GO

-- 删除旧表（如果存在）
IF OBJECT_ID('dbo.BusTrip', 'U') IS NOT NULL
    DROP TABLE dbo.BusTrip;
GO

-- 创建公交行程表
CREATE TABLE dbo.BusTrip (
    -- 主键
    SIGNID NVARCHAR(100) NOT NULL PRIMARY KEY,  -- 唯一记录ID
    
    -- 时间维度
    OPD_DATE DATE NOT NULL,                      -- 运营日期
    WEEKDAY INT NOT NULL CHECK (WEEKDAY BETWEEN 1 AND 7),  -- 星期几
    BLOCK NVARCHAR(50) NOT NULL,                 -- 时间段
    
    -- 线路信息
    LINEABBR NVARCHAR(20) NOT NULL,              -- 线路ID
    DIRECTION NVARCHAR(20) NOT NULL,             -- 方向
    TRIP_ID_INT NVARCHAR(100) NOT NULL,          -- 行程ID
    
    -- 站点信息
    STOPABBR NVARCHAR(50),                       -- 站点缩写
    SEQUENCE INT,                                -- 站点序号
    
    -- 时间指标
    SCHED_TRIP_TIME DECIMAL(10,2),               -- 计划行程时间（秒）
    ACT_TRIP_TIME DECIMAL(10,2),                 -- 实际行程时间（秒）
    DIFF_TRIP_TIME DECIMAL(10,2),                -- 行程时间差（秒）
    
    -- 距离和速度
    SCHED_DISTANCE DECIMAL(10,2),                -- 计划距离（米）
    ACT_SPEED DECIMAL(6,2),                      -- 实际速度（km/h）
    
    -- 车辆状态
    IS_ADDITIONAL BIT NOT NULL DEFAULT 0,        -- 是否为增派车辆
    
    -- 到站和离站时间
    SCHED_ARR_TIME DECIMAL(10,2),                -- 计划到达时间（秒）
    ACT_ARR_TIME DECIMAL(10,2),                  -- 实际到达时间（秒）
    SCHED_DEP_TIME DECIMAL(10,2),                -- 计划离站时间（秒）
    ACT_DEP_TIME DECIMAL(10,2),                  -- 实际离站时间（秒）
    DIFF_DEP_TIME DECIMAL(10,2),                 -- 离站时间差（秒）
    
    -- 停站信息
    DWELL_TIME DECIMAL(10,2),                    -- 停站时间（秒）
    DISTANCE_TO_NEXT DECIMAL(10,2),              -- 到下一站距离（米）
    DISTANCE_TO_TRIP DECIMAL(10,2),              -- 到终点站距离（米）
    
    -- 乘客信息
    BOARDING INT NOT NULL DEFAULT 0,             -- 上车人数
    ALIGHTING INT NOT NULL DEFAULT 0,            -- 下车人数
    LOAD INT NOT NULL DEFAULT 0,                 -- 车载人数
    WHEELCHAIR_COUNT INT NOT NULL DEFAULT 0,     -- 轮椅乘客数
    
    -- 审计字段
    CREATED_AT DATETIME2 DEFAULT GETDATE(),      -- 创建时间
    UPDATED_AT DATETIME2 DEFAULT GETDATE()       -- 更新时间
);
GO

-- 创建索引以提高查询性能
CREATE NONCLUSTERED INDEX IX_BusTrip_OPD_DATE 
    ON dbo.BusTrip(OPD_DATE);
GO

CREATE NONCLUSTERED INDEX IX_BusTrip_LINEABBR 
    ON dbo.BusTrip(LINEABBR);
GO

CREATE NONCLUSTERED INDEX IX_BusTrip_TRIP_ID_INT 
    ON dbo.BusTrip(TRIP_ID_INT);
GO

CREATE NONCLUSTERED INDEX IX_BusTrip_BLOCK 
    ON dbo.BusTrip(BLOCK);
GO

-- 创建复合索引
CREATE NONCLUSTERED INDEX IX_BusTrip_Line_Date 
    ON dbo.BusTrip(LINEABBR, OPD_DATE) 
    INCLUDE (BOARDING, ALIGHTING, LOAD);
GO

-- 创建汇总视图
CREATE VIEW dbo.vw_DailyLineSummary AS
SELECT 
    OPD_DATE,
    LINEABBR,
    DIRECTION,
    COUNT(DISTINCT TRIP_ID_INT) AS TotalTrips,
    SUM(BOARDING) AS TotalBoardings,
    SUM(ALIGHTING) AS TotalAlightings,
    AVG(DIFF_DEP_TIME) AS AvgDelay,
    AVG(DWELL_TIME) AS AvgDwellTime,
    AVG(ACT_SPEED) AS AvgSpeed,
    MAX(LOAD) AS MaxLoad,
    SUM(WHEELCHAIR_COUNT) AS TotalWheelchairs,
    SUM(CASE WHEN IS_ADDITIONAL = 1 THEN 1 ELSE 0 END) AS AdditionalBuses
FROM dbo.BusTrip
GROUP BY OPD_DATE, LINEABBR, DIRECTION;
GO

-- 创建时段汇总视图
CREATE VIEW dbo.vw_BlockSummary AS
SELECT 
    BLOCK,
    LINEABBR,
    COUNT(*) AS RecordCount,
    AVG(LOAD) AS AvgLoad,
    AVG(DWELL_TIME) AS AvgDwellTime,
    AVG(DIFF_DEP_TIME) AS AvgDelay
FROM dbo.BusTrip
GROUP BY BLOCK, LINEABBR;
GO

-- 创建存储过程：数据质量检查
CREATE PROCEDURE dbo.sp_CheckDataQuality
    @StartDate DATE = NULL,
    @EndDate DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    -- 默认检查最近7天
    IF @StartDate IS NULL
        SET @StartDate = DATEADD(DAY, -7, GETDATE());
    IF @EndDate IS NULL
        SET @EndDate = GETDATE();
    
    -- 检查异常速度
    SELECT 'Abnormal Speed' AS Issue, COUNT(*) AS Count
    FROM dbo.BusTrip
    WHERE OPD_DATE BETWEEN @StartDate AND @EndDate
        AND (ACT_SPEED > 120 OR ACT_SPEED < 0);
    
    -- 检查异常停站时间
    SELECT 'Abnormal Dwell Time' AS Issue, COUNT(*) AS Count
    FROM dbo.BusTrip
    WHERE OPD_DATE BETWEEN @StartDate AND @EndDate
        AND (DWELL_TIME > 600 OR DWELL_TIME < 0);
    
    -- 检查乘客数据一致性
    SELECT 'Passenger Count Inconsistency' AS Issue, COUNT(*) AS Count
    FROM dbo.BusTrip
    WHERE OPD_DATE BETWEEN @StartDate AND @EndDate
        AND WHEELCHAIR_COUNT > LOAD;
    
    -- 检查重复记录
    SELECT 'Duplicate Records' AS Issue, COUNT(*) - COUNT(DISTINCT SIGNID) AS Count
    FROM dbo.BusTrip
    WHERE OPD_DATE BETWEEN @StartDate AND @EndDate;
END;
GO

-- 创建触发器：更新时间戳
CREATE TRIGGER trg_UpdateTimestamp
ON dbo.BusTrip
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE dbo.BusTrip
    SET UPDATED_AT = GETDATE()
    FROM dbo.BusTrip t
    INNER JOIN inserted i ON t.SIGNID = i.SIGNID;
END;
GO

-- 授权（根据需要调整）
-- GRANT SELECT, INSERT, UPDATE ON dbo.BusTrip TO [spark_user];
-- GO

PRINT '数据库表创建完成！';
GO 