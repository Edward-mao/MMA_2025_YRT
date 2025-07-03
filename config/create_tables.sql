-- Bus trip data table creation script
-- For SQL Server 2016+

-- Create database (if not exists)
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'BusSim')
BEGIN
    CREATE DATABASE BusSim
    COLLATE SQL_Latin1_General_CP1_CI_AS;
END
GO

USE BusSim;
GO

-- Create schema (if not exists)
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'dbo')
BEGIN
    EXEC('CREATE SCHEMA dbo');
END
GO

-- Drop old table (if exists)
IF OBJECT_ID('dbo.BusTrip', 'U') IS NOT NULL
    DROP TABLE dbo.BusTrip;
GO

-- Create bus trip table
CREATE TABLE dbo.BusTrip (
    -- Primary key
    SIGNID NVARCHAR(100) NOT NULL PRIMARY KEY,  -- Unique record ID
    
    -- Time dimensions
    OPD_DATE DATE NOT NULL,                      -- Operating date
    WEEKDAY INT NOT NULL CHECK (WEEKDAY BETWEEN 1 AND 7),  -- Day of week
    BLOCK NVARCHAR(50) NOT NULL,                 -- Time block
    
    -- Route information
    LINEABBR NVARCHAR(20) NOT NULL,              -- Route ID
    DIRECTION NVARCHAR(20) NOT NULL,             -- Direction
    TRIP_ID_INT NVARCHAR(100) NOT NULL,          -- Trip ID
    
    -- Stop information
    STOPABBR NVARCHAR(50),                       -- Stop abbreviation
    SEQUENCE INT,                                -- Stop sequence number
    
    -- Time metrics
    SCHED_TRIP_TIME DECIMAL(10,2),               -- Scheduled trip time (seconds)
    ACT_TRIP_TIME DECIMAL(10,2),                 -- Actual trip time (seconds)
    DIFF_TRIP_TIME DECIMAL(10,2),                -- Trip time difference (seconds)
    
    -- Distance and speed
    SCHED_DISTANCE DECIMAL(10,2),                -- Scheduled distance (meters)
    ACT_SPEED DECIMAL(6,2),                      -- Actual speed (km/h)
    
    -- Vehicle status
    IS_ADDITIONAL BIT NOT NULL DEFAULT 0,        -- Whether additional vehicle
    
    -- Arrival and departure times
    SCHED_ARR_TIME DECIMAL(10,2),                -- Scheduled arrival time (seconds)
    ACT_ARR_TIME DECIMAL(10,2),                  -- Actual arrival time (seconds)
    SCHED_DEP_TIME DECIMAL(10,2),                -- Scheduled departure time (seconds)
    ACT_DEP_TIME DECIMAL(10,2),                  -- Actual departure time (seconds)
    DIFF_DEP_TIME DECIMAL(10,2),                 -- Departure time difference (seconds)
    
    -- Stop information
    DWELL_TIME DECIMAL(10,2),                    -- Dwell time (seconds)
    DISTANCE_TO_NEXT DECIMAL(10,2),              -- Distance to next stop (meters)
    DISTANCE_TO_TRIP DECIMAL(10,2),              -- Distance to trip end (meters)
    
    -- Passenger information
    BOARDING INT NOT NULL DEFAULT 0,             -- Boarding count
    ALIGHTING INT NOT NULL DEFAULT 0,            -- Alighting count
    LOAD INT NOT NULL DEFAULT 0,                 -- Load count
    WHEELCHAIR_COUNT INT NOT NULL DEFAULT 0,     -- Wheelchair passenger count
    
    -- Audit fields
    CREATED_AT DATETIME2 DEFAULT GETDATE(),      -- Creation time
    UPDATED_AT DATETIME2 DEFAULT GETDATE()       -- Update time
);
GO

-- Create indexes to improve query performance
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

-- Create composite index
CREATE NONCLUSTERED INDEX IX_BusTrip_Line_Date 
    ON dbo.BusTrip(LINEABBR, OPD_DATE) 
    INCLUDE (BOARDING, ALIGHTING, LOAD);
GO

-- Create summary view
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

-- Create time block summary view
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

-- Create stored procedure: data quality check
CREATE PROCEDURE dbo.sp_CheckDataQuality
    @StartDate DATE = NULL,
    @EndDate DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Default check last 7 days
    IF @StartDate IS NULL
        SET @StartDate = DATEADD(DAY, -7, GETDATE());
    IF @EndDate IS NULL
        SET @EndDate = GETDATE();
    
    -- Check abnormal speed
    SELECT 'Abnormal Speed' AS Issue, COUNT(*) AS Count
    FROM dbo.BusTrip
    WHERE OPD_DATE BETWEEN @StartDate AND @EndDate
        AND (ACT_SPEED > 120 OR ACT_SPEED < 0);
    
    -- Check abnormal dwell time
    SELECT 'Abnormal Dwell Time' AS Issue, COUNT(*) AS Count
    FROM dbo.BusTrip
    WHERE OPD_DATE BETWEEN @StartDate AND @EndDate
        AND (DWELL_TIME > 600 OR DWELL_TIME < 0);
    
    -- Check passenger data consistency
    SELECT 'Passenger Count Inconsistency' AS Issue, COUNT(*) AS Count
    FROM dbo.BusTrip
    WHERE OPD_DATE BETWEEN @StartDate AND @EndDate
        AND WHEELCHAIR_COUNT > LOAD;
    
    -- Check duplicate records
    SELECT 'Duplicate Records' AS Issue, COUNT(*) - COUNT(DISTINCT SIGNID) AS Count
    FROM dbo.BusTrip
    WHERE OPD_DATE BETWEEN @StartDate AND @EndDate;
END;
GO

-- Create trigger: update timestamp
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

-- Grant permissions (adjust as needed)
-- GRANT SELECT, INSERT, UPDATE ON dbo.BusTrip TO [spark_user];
-- GO

PRINT 'Database table creation completed!';
GO 