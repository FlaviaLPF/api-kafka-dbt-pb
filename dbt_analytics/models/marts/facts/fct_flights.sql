{{ config(
    materialized='incremental',
    unique_key='flight_pk',
    as_columnstore=false
) }}

-- 1. Seleção dos dados da staging com filtro incremental
with staging as (
    select 
        aircraft_id,
        position_timestamp,
        latitude,
        longitude,
        speed_kmh,
        altitude_baro,
        vertical_rate,
        on_ground,
        inserted_at
    from {{ ref('stg_flights') }}

    {% if is_incremental() %}
      -- Filtro para processar apenas dados novos
      where inserted_at > (select max(ingestion_timestamp) from {{ this }})
    {% endif %}
),

-- 2. Join Geográfico com Margem de Erro (0.005 graus ~ aprox. 500 metros)
joined as (
    select
        -- PK da Tabela Fato
        cast(s.aircraft_id as varchar(50)) + '-' + convert(varchar, s.position_timestamp, 120) as flight_pk,
        
        -- FK: aircraft_id
        cast(s.aircraft_id as varchar(50)) as aircraft_id,
        
        -- FK: flight_date
        cast(s.position_timestamp as date) as flight_date,
        
        -- FK: airport_code com tratamento de N/A
        isnull(a.airport_code, 'N/A') as airport_code,

        -- Métricas e Atributos
        s.latitude,
        s.longitude,
        isnull(s.speed_kmh, 0) as speed_kmh,
        isnull(s.altitude_baro, 0) as altitude_baro,
        isnull(s.vertical_rate, 0) as vertical_rate,
        s.on_ground,
        
        -- Lógica de Status
        case 
            when s.on_ground = 1 then 'No Solo' 
            else 'Em Voo' 
        end as flight_status,

        -- Lógica de Comportamento (Baseada em Altitude e Taxa Vertical)
        case 
            when s.on_ground = 1 then 'Táxi / Estacionado'
            when s.altitude_baro > 15000 then 'Cruzeiro' -- Aviões acima de 15k pés
            when s.vertical_rate > 500 then 'Subida'
            when s.vertical_rate < -500 then 'Descida / Aproximação'
            else 'Nivelado'
        end as flight_behavior,

        -- B. Hierarquia de Performance: Faixas de Altitude
        case 
            when s.on_ground = 1 then 'Solo'
            when s.altitude_baro < 10000 then 'Baixa Altitude (< 10k ft)'
            when s.altitude_baro between 10000 and 25000 then 'Média Altitude (10k-25k ft)'
            else 'Alta Altitude (> 25k ft)'
        end as altitude_tier,

        -- B. Hierarquia de Performance: Faixas de Velocidade
        case 
            when s.on_ground = 1 then 'Solo'
            when s.speed_kmh < 400 then 'Lento / Aproximação'
            when s.speed_kmh between 400 and 700 then 'Cruzeiro Médio'
            else 'Alta Velocidade'
        end as speed_tier,
        
        s.inserted_at as ingestion_timestamp,
        datepart(hour, s.position_timestamp) as hour_24

    from staging s
    left join {{ ref('dim_airports') }} a 
        -- Aplicando margem de segurança nas coordenadas para não perder aviões no solo
        on s.latitude between (a.lat_min - 0.005) and (a.lat_max + 0.005)
        and s.longitude between (a.lon_min - 0.005) and (a.lon_max + 0.005)
)

select * from joined