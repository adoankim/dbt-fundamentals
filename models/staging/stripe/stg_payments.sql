with payments as (
    select
        orderid as order_id,
        paymentmethod as payment_method,
        status,
        amount,
        created
    from dbt-tutorial.stripe.payment
)

select * from payments