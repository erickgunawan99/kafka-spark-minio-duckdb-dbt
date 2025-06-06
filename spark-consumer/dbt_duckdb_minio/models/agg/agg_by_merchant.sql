with agg_by_merchant as (

    select
        merchant_id,
        sum(quantity),
        sum(amount),
    from {{ ref('raw_transaction_view') }}
    group by merchant_id

)

select * from agg_by_merchant