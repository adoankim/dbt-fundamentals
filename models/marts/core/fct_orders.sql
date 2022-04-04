with orders as (
    select * from {{ ref("stg_orders") }}
),
payments as (
    select 
        order_id,
        sum(IF(status = 'success', amount, 0)) amount
    from {{ ref("stg_payments")}}
    group by order_id
),
final as (
    select
        orders.customer_id,
        orders.order_id,
        ifnull(payments.amount, 0) amount,
    from orders
    left join 
        payments on payments.order_id = orders.order_id

)

select * from final