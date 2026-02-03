{{ config(
    materialized='table',
    as_columnstore=false
) }}

with staging as (
    select 
        aircraft_id,
        -- Tratamos possíveis nulos ou strings vazias aqui
        coalesce(origin_country, 'Desconhecido') as origin_country
    from {{ ref('stg_flights') }}
),

deduplicated as (
    select 
        cast(aircraft_id as varchar(50)) as aircraft_id,
        origin_country,
        -- Ordenamos para garantir que pegamos o país preenchido caso haja variação
        row_number() over (partition by aircraft_id order by origin_country desc) as rn
    from staging
)

select
    aircraft_id, 
    origin_country,
    -- Categoria: Baseada no Brasil
    case 
        when origin_country = 'Brazil' then 'Nacional'
        when origin_country = 'Desconhecido' then 'Não Identificado'
        else 'Estrangeiro'
    end as aircraft_category,
    -- Continente: Mapeamento Geográfico
    case 
        when origin_country = 'Brazil' then 'América do Sul'
        when origin_country in ('Argentina', 'Chile', 'Uruguay', 'Colombia', 'Peru') then 'América do Sul'
        when origin_country in ('United States', 'Canada', 'Mexico') then 'América do Norte'
        when origin_country in ('France', 'Germany', 'United Kingdom', 'Portugal', 'Spain', 'Italy', 'Switzerland') then 'Europa'
        when origin_country = 'Desconhecido' then 'Desconhecido'
        else 'Outros Continentes'
    end as continent
from deduplicated
where rn = 1