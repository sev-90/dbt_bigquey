{{ config(materialized='view', alias='transformed_orders') }}

with source as (

    select *
    -- from {{ ref('orders') }}  -- ref the seeded table
    from {{source('raw_data_bq','orders')}}
),

transformed as (

    select
        order_id,
        customer_id,
        order_date,
        status,
        amount,
        -- derived column: year-month for grouping
        format_date('%Y-%m', order_date) as order_month,
        -- flag cancelled
        case when status = 'Cancelled' then true else false end as is_cancelled,
        -- bucket orders by value
        case
            when amount < 100 then 'Low'
            when amount between 100 and 300 then 'Medium'
            else 'High'
        end as order_value_bucket
    from source

)

select * from transformed