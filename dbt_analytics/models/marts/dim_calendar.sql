{{ config(
    materialized='table',
    as_columnstore=false
) }}

with dates as (
    select distinct 
        -- 1. Chave e Data Base (ÚNICA por dia)
        cast(position_timestamp as date) as flight_date,
        
        -- 2. Hierarquia de Data
        datepart(year, position_timestamp) as year_num,
        'Q' + cast(datepart(quarter, position_timestamp) as varchar) as quarter_name,
        datepart(month, position_timestamp) as month_num,
        -- Removi o nome do mês por extenso para evitar problemas de idioma (Jan vs Janeiro)
        -- Você pode tratar isso no Power BI ou usar formatadores numéricos
        datepart(week, position_timestamp) as week_of_year,
        datepart(day, position_timestamp) as day_num,
        
        -- 3. Atributos de Dia
        datepart(weekday, position_timestamp) as day_of_week_num
    from {{ ref('stg_flights') }}
)

select * from dates