with source_data as (

    select 
        transactionId as transaction_id,
        merchantId as merchant_id,
        product,
        quantity,
        customerId as customer_id,
        amount,
        transactionTime as transaction_time,
        paymentType as payment_type,
        date,
        time
     from {{ source('minio_transaction', 'transaction') }}

)

select *
from source_data