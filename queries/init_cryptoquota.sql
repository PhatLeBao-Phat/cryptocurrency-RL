-- DB INIT 
CREATE TABLE IF NOT EXISTS fact_cryptoquota (
    "symbol" VARCHAR(10),                -- Cryptocurrency symbol (e.g., BTC)
    "name" VARCHAR(100),                 -- Full name of the cryptocurrency
    "price" DECIMAL(18, 8),              -- Current price
    "changesPercentage" DECIMAL(10, 2),  -- Percentage change
    "change" DECIMAL(18, 8),             -- Absolute change in price
    "dayLow" DECIMAL(18, 8),             -- Lowest price of the day
    "dayHigh" DECIMAL(18, 8),            -- Highest price of the day
    "yearHigh" DECIMAL(18, 8),           -- Highest price of the year
    "yearLow" DECIMAL(18, 8),            -- Lowest price of the year
    "marketCap" BIGINT,                  -- Market capitalization
    "priceAvg50" DECIMAL(18, 8),         -- 50-day average price
    "priceAvg200" DECIMAL(18, 8),        -- 200-day average price
    "exchange" VARCHAR(50),              -- Exchange where the crypto is traded
    "volume" BIGINT,                     -- Trading volume
    "avgVolume" BIGINT,                  -- Average trading volume
    "open" DECIMAL(18, 8),               -- Opening price
    "previousClose" DECIMAL(18, 8),      -- Previous closing price
    "eps" DECIMAL(10, 2),                -- Earnings per share (if applicable)
    "pe" DECIMAL(10, 2),                 -- Price-to-earnings ratio (if applicable)
    "earningsAnnouncement" TIMESTAMP,    -- Earnings announcement date (if applicable)
    "sharesOutstanding" BIGINT,          -- Number of shares outstanding
    "timestamp" TIMESTAMP,               -- Timestamp of the data
    "ingestionTime" TIMESTAMP            -- IngestionTime of the data
);