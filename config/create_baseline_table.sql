-- 创建Baseline表脚本
-- 该表用于存储基线场景的数据，结构与BusTrip表完全相同

USE BusSim;
GO

-- 删除旧表（如果存在）
IF OBJECT_ID('dbo.Baseline', 'U') IS NOT NULL
    DROP TABLE dbo.Baseline;
GO

-- 创建Baseline表
CREATE TABLE dbo.Baseline (
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
CREATE NONCLUSTERED INDEX IX_Baseline_OPD_DATE 
    ON dbo.Baseline(OPD_DATE);
GO

CREATE NONCLUSTERED INDEX IX_Baseline_LINEABBR 
    ON dbo.Baseline(LINEABBR);
GO

CREATE NONCLUSTERED INDEX IX_Baseline_TRIP_ID_INT 
    ON dbo.Baseline(TRIP_ID_INT);
GO

CREATE NONCLUSTERED INDEX IX_Baseline_BLOCK 
    ON dbo.Baseline(BLOCK);
GO

-- 创建复合索引
CREATE NONCLUSTERED INDEX IX_Baseline_Line_Date 
    ON dbo.Baseline(LINEABBR, OPD_DATE) 
    INCLUDE (BOARDING, ALIGHTING, LOAD);
GO

-- 创建触发器：更新时间戳
CREATE TRIGGER trg_Baseline_UpdateTimestamp
ON dbo.Baseline
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE dbo.Baseline
    SET UPDATED_AT = GETDATE()
    FROM dbo.Baseline t
    INNER JOIN inserted i ON t.SIGNID = i.SIGNID;
END;
GO

PRINT 'Baseline表创建完成！';
GO 