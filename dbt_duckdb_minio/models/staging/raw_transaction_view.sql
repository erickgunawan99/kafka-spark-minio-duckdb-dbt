with source_data as (

    select 
        transactionId as transaction_id,
        merchantId as merchant_id,
        product,
        quantity,
        customerId as customer_id,
        amount,
        amount * quantity as total_amount,
        transactionTime as transaction_time,
        paymentType as payment_type,
        date,
        time
     from {{ source('minio_transaction', 'transaction') }} 
     qualify row_number() over (partition by transaction_id) = 1

)

select *
from source_data