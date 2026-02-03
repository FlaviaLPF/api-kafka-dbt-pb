with source as (
    select * from {{ source('opensky', 'flights_raw') }}
),

cleaned as (
    select
        cast(upper(trim(icao24)) as varchar(50)) as aircraft_id,
        cast(upper(trim(replace(callsign, '-', ''))) as varchar(50)) as flight_number,
        cast(id_surrogate as int) as id_surrogate,
        trim(origin_country) as origin_country,
        
        -- Conversão de Timestamp
        cast(dateadd(s, cast(time_position as bigint), '1970-01-01') as datetime2) as position_timestamp,
        
        -- Telemetria
        cast(longitude as float) as longitude,
        cast(latitude as float) as latitude,
        cast(baro_altitude as float) as altitude_baro,
        cast(velocity as float) as speed_kmh,

        -- MAPEAMENTO DAS NOVAS COLUNAS (O que faltava!)
        cast(vertical_rate as float) as vertical_rate, 
        inserted_at, 
        
        on_ground,
        cast(processing_date as datetime) as processing_date,

        row_number() over (
            partition by icao24, time_position 
            order by inserted_at desc -- Usando inserted_at para pegar o registro mais novo
        ) as row_num

    from source
)

select 
    aircraft_id,
    flight_number,
    id_surrogate,
    origin_country,
    position_timestamp,
    longitude,
    latitude,
    altitude_baro,
    speed_kmh,
    vertical_rate,
    on_ground,
    processing_date,
    inserted_at
from cleaned 
where row_num = 1