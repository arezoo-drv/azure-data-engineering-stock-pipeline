CREATE or alter PROCEDURE   [stg].[usp_load_stock_candle_daily]
    @SinceUtc DATETIME2(0) = NULL   -- if it is NULL then read from watermark ‌
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @LastUtc DATETIME2(0);
BEGIN TRY
   BEGIN TRAN;

     -- 1) read watermark (locked)
    SELECT @LastUtc =
        COALESCE(@SinceUtc,
                 (SELECT last_success_utc
                  FROM stg.etl_watermark WITH (UPDLOCK, HOLDLOCK)
                  WHERE process_name = 'load_stock_candle_daily'),
                 '1900-01-01');

    -- 2) compute max ingested for this delta
    DECLARE @MaxIngestedUtc DATETIME2(0);

    SELECT @MaxIngestedUtc = MAX(ingested_at_utc)
                             FROM raw.stock_candle_daily
                             WHERE ingested_at_utc > @LastUtc;

    IF @MaxIngestedUtc IS NULL
        BEGIN
            COMMIT;
            RETURN;
        END

    /* last varsion per symbol/date از delta */
    /* 1) Delta + Dedup → materialize */
    IF OBJECT_ID('tempdb..#src') IS NOT NULL DROP TABLE #src;

      -- 3) delta + dedup
    SELECT
        r.symbol,
        r.candle_date,
        CAST(r.open_price  AS DECIMAL(19,6)) AS open_price,
        CAST(r.high_price  AS DECIMAL(19,6)) AS high_price,
        CAST(r.low_price   AS DECIMAL(19,6)) AS low_price,
        CAST(r.close_price AS DECIMAL(19,6)) AS close_price,
        CAST(ISNULL(r.volume,0) AS BIGINT)   AS volume,
        r.source_name
    INTO #src
    FROM (
        SELECT
            r.*,
            ROW_NUMBER() OVER (
                PARTITION BY r.symbol, r.candle_date,r.source_name
                ORDER BY r.ingested_at_utc DESC, r.raw_id DESC
            ) AS rn
        FROM raw.stock_candle_daily r
        WHERE r.ingested_at_utc > @LastUtc
    ) r
    WHERE r.rn = 1;

    -- 4) UPDATE only changed
    UPDATE t
       SET t.open_price     = s.open_price,
           t.high_price     = s.high_price,
           t.low_price      = s.low_price,
           t.close_price    = s.close_price,
           t.volume         = s.volume,
           t.load_batch_utc = SYSUTCDATETIME(),
           t.source_name    = s.source_name
    FROM stg.stock_candle_daily t
    JOIN #src s
      ON s.symbol = t.symbol
     AND s.candle_date = t.candle_date
     AND s.source_name = t.source_name
    WHERE
        (t.open_price  <> s.open_price OR
         t.high_price  <> s.high_price OR
         t.low_price   <> s.low_price  OR
         t.close_price <> s.close_price OR
         t.volume      <> s.volume );

    -- 5) INSERT new
    INSERT INTO stg.stock_candle_daily
        (symbol, candle_date, open_price, high_price, low_price, close_price, volume,source_name)
    SELECT
        s.symbol, s.candle_date, s.open_price, s.high_price, s.low_price, s.close_price, s.volume,s.source_name
    FROM #src s
    LEFT JOIN stg.stock_candle_daily t
      ON t.symbol = s.symbol
     AND t.candle_date = s.candle_date
     AND t.source_name = s.source_name
    WHERE t.stg_id  IS NULL;

    -- 6) upsert watermark (inside same tran)
    IF EXISTS (SELECT 1 FROM stg.etl_watermark WHERE process_name='load_stock_candle_daily')
        UPDATE stg.etl_watermark
           SET last_success_utc = @MaxIngestedUtc
         WHERE process_name='load_stock_candle_daily';
    ELSE
        INSERT INTO stg.etl_watermark(process_name, last_success_utc)
        VALUES ('load_stock_candle_daily', @MaxIngestedUtc);

 COMMIT;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;
        THROW;
    END CATCH
 END