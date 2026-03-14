CREATE SCHEMA raw;
GO
CREATE SCHEMA stg;
GO
CREATE SCHEMA dwh;
GO
CREATE SCHEMA audit;
GO

CREATE TABLE audit.ingestion_runs (
    run_id              UNIQUEIDENTIFIER NOT NULL,
    orchestration_id    NVARCHAR(100) NULL,
    run_started_at      DATETIME2(0) NOT NULL,
    run_ended_at        DATETIME2(0) NOT NULL,
    status              NVARCHAR(20) NOT NULL,     -- Success/Failed/Partial
    symbols             NVARCHAR(200) NOT NULL,    -- "AAPL,RY,TD,SU"
    rows_inserted_raw   INT NOT NULL,
    error_message       NVARCHAR(MAX) NULL,
    CONSTRAINT PK_ingestion_runs PRIMARY KEY (run_id)
);

/* 1) API Call Log: audit Errors */
CREATE TABLE raw.api_call_log (
    api_call_id      BIGINT IDENTITY(1,1) PRIMARY KEY,
    source_name      NVARCHAR(50) NOT NULL,      -- 'finnhub'
    endpoint_name    NVARCHAR(50) NOT NULL,      -- 'stock/candle'
    symbol           NVARCHAR(20) NOT NULL,      -- e.g., 'RY.TO'
    resolution       NVARCHAR(10) NULL,          -- 'D'
    period_start_utc DATETIME2(0) NULL,
    period_end_utc   DATETIME2(0) NULL,
    http_status      INT NULL,
    is_success       BIT NOT NULL DEFAULT 0,
    error_message    NVARCHAR(4000) NULL,
    requested_at_utc DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME(),
    response_json    NVARCHAR(MAX) NULL,
    run_id              UNIQUEIDENTIFIER NOT NULL
);
GO
------------- Raw Table

/* 2) RAW Daily OHLCV:    */

CREATE TABLE raw.stock_candle_daily (
    raw_id          BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    symbol          NVARCHAR(20) NOT NULL,
    candle_date     DATE NOT NULL,
    open_price      DECIMAL(19,6) NULL,
    high_price      DECIMAL(19,6) NULL,
    low_price       DECIMAL(19,6) NULL,
    close_price     DECIMAL(19,6) NULL,
    volume          BIGINT NULL,
    source_name     NVARCHAR(50) NOT NULL DEFAULT 'twelvedata',
    ingested_at_utc DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME(),
    api_call_id     BIGINT NULL
  
);
GO




CREATE INDEX IX_raw_symbol_date_ingested
ON raw.stock_candle_daily (symbol, candle_date, ingested_at_utc DESC);
GO

ALTER TABLE raw.stock_candle_daily
ADD CONSTRAINT FK_raw_stock_candle_daily_api_call
FOREIGN KEY (api_call_id) REFERENCES raw.api_call_log(api_call_id);
GO
------------------------- STG Tables

CREATE TABLE stg.stock_candle_daily (
    symbol          NVARCHAR(20) NOT NULL,
    candle_date     DATE NOT NULL,
    open_price      DECIMAL(19,6) NOT NULL,
    high_price      DECIMAL(19,6) NOT NULL,
    low_price       DECIMAL(19,6) NOT NULL,
    close_price     DECIMAL(19,6) NOT NULL,
    volume          BIGINT NOT NULL,
    load_batch_utc  DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_stg_stock_candle_daily PRIMARY KEY (symbol, candle_date)
);
GO


IF OBJECT_ID('stg.etl_watermark','U') IS NULL
BEGIN
    CREATE TABLE stg.etl_watermark (
        process_name       NVARCHAR(200) NOT NULL PRIMARY KEY,
        last_success_utc   DATETIME2(0)   NOT NULL
    );
END


CREATE TABLE [stg].[etl_watermark](
	[process_name] [nvarchar](200) NOT NULL,
	[last_success_utc] [datetime2](0) NOT NULL,
PRIMARY KEY CLUSTERED 
(
	[process_name] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO


----------------dataware house
CREATE TABLE dw.DimDate
(
    DateKey             INT         NOT NULL PRIMARY KEY,   -- yyyymmdd
    FullDate            DATE        NOT NULL,

    [Year]              SMALLINT    NOT NULL,
    [Quarter]           TINYINT     NOT NULL,                -- 1..4
    QuarterName         NVARCHAR(10)NOT NULL,                -- Q1..Q4
    QuarterLabel        NVARCHAR(30)NOT NULL,                -- 2026 Q1

    [Month]             TINYINT     NOT NULL,                -- 1..12
    MonthName           NVARCHAR(20)NOT NULL,                -- January...
    MonthNameShort      NVARCHAR(10)NOT NULL,                -- Jan...
    MonthLabel          NVARCHAR(30)NOT NULL,                -- 2026-02 February
    MonthStartDate      DATE        NOT NULL,
    MonthEndDate        DATE        NOT NULL,

    [Day]               TINYINT     NOT NULL,                -- 1..31
    DayOfYear           SMALLINT    NOT NULL,                -- 1..365/366

    DayOfWeekISO        TINYINT     NOT NULL,                -- 1..7 (Mon=1)
    DayName             NVARCHAR(20)NOT NULL,                -- Monday...
    DayNameShort        NVARCHAR(10)NOT NULL,                -- Mon...
    IsWeekend           BIT         NOT NULL,                -- Sat/Sun
    IsMonthStart        BIT         NOT NULL,
    IsMonthEnd          BIT         NOT NULL
);
GO

CREATE TABLE dw.DimSymbol (
    SymbolKey INT IDENTITY(1,1) PRIMARY KEY,
    Symbol NVARCHAR(20) UNIQUE
);
GO


CREATE TABLE dw.FactStockCandleDaily (
    DateKey INT,
    SymbolKey INT,
    SourceRecordId [bigint] NULL,
    OpenPrice DECIMAL(19,6),
    HighPrice DECIMAL(19,6),
    LowPrice DECIMAL(19,6),
    ClosePrice DECIMAL(19,6),
    Volume BIGINT,
    SourceName NVARCHAR(50),

    CONSTRAINT PK_FactStock PRIMARY KEY (DateKey, SymbolKey)
);
GO
