with agg_by_merchant as (

    select
        merchant_id,
        sum(quantity) as total_product_sold,
        sum(total_amount) as revenue,
    from {{ ref('raw_transaction_view') }}
    group by merchant_id
    order by revenue

)

select * from agg_by_merchant