-- Create Baseline table script
-- This table is used to store baseline scenario data, structure identical to BusTrip table

USE BusSim;
GO

-- Drop old table (if exists)
IF OBJECT_ID('dbo.Baseline', 'U') IS NOT NULL
    DROP TABLE dbo.Baseline;
GO

-- Create Baseline table
CREATE TABLE dbo.Baseline (
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

-- Create composite index
CREATE NONCLUSTERED INDEX IX_Baseline_Line_Date 
    ON dbo.Baseline(LINEABBR, OPD_DATE) 
    INCLUDE (BOARDING, ALIGHTING, LOAD);
GO

-- Create trigger: update timestamp
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

PRINT 'Baseline table creation completed!';
GO 