-USE DWFlights;
GO

-- 1. Verifica se o Schema já existe antes de criar
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'raw')
BEGIN
    EXEC('CREATE SCHEMA raw');
END
GO

-- 2. Verifica se a tabela já existe antes de criar
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[raw].[flights_raw]') AND type in (N'U'))
BEGIN
    CREATE TABLE raw.flights_raw (
        id_surrogate INT IDENTITY(1,1) PRIMARY KEY,
        icao24 VARCHAR(50),
        callsign VARCHAR(50),
        origin_country VARCHAR(100),
        time_position BIGINT,
        last_contact BIGINT,
        longitude FLOAT,
        latitude FLOAT,
        baro_altitude FLOAT,
        on_ground BIT,
        velocity FLOAT,
        heading FLOAT,
        vertical_rate FLOAT,
        sensors VARCHAR(MAX),
        geo_altitude FLOAT,
        squawk VARCHAR(50),
        spi BIT,
        position_source BIGINT,
        processing_date DATE,
        inserted_at DATETIME DEFAULT GETDATE()
    );
END
GO


