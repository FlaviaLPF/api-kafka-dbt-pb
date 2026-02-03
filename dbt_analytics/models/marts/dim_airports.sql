{{ config(
    materialized='table',
    as_columnstore=false
) }}

with base as (
    select 
        'Sudeste' as region, 
        'Sao Paulo' as state, 
        'Sao Paulo' as city, 
        'GRU' as airport_code, 
        'Guarulhos International' as airport_name, 
        -23.55 as lat_min, -23.35 as lat_max, -46.60 as lon_min, -46.30 as lon_max
    union all
    select 
        'Sudeste' as region, 
        'Sao Paulo' as state, 
        'Sao Paulo' as city, 
        'CGH' as airport_code, 
        'Congonhas' as airport_name, 
        -23.68 as lat_min, -23.60 as lat_max, -46.70 as lon_min, -46.50 as lon_max
    union all
    
    select 
        'Sudeste' as region, 
        'Sao Paulo' as state, 
        'Campinas' as city, 
        'VCP' as airport_code, -- Use VCP ou SBKP, mas VCP é mais comum em dashboards
        'Viracopos' as airport_name, 
        -23.05 as lat_min, -22.95 as lat_max, -47.18 as lon_min, -47.10 as lon_max
    union all
    select 
        'Não Mapeado' as region, 
        'Não Mapeado' as state, 
        'Em Rota' as city, 
        'N/A' as airport_code, 
        'Espaço Aéreo Geral' as airport_name, 
        0 as lat_min, 0 as lat_max, 0 as lon_min, 0 as lon_max
)

select * from base