with payments as (
    select
        orderid as order_id,
        paymentmethod as payment_method,
        status,
        {{ cents_to_dollars('amount') }} amount,
        created
    from {{ source('stripe', 'payment') }}
)

select * from payments