{%- set payment_methods = ['credit_card', 'bank_transfer', 'coupon', 'gift_card'] -%}

with payments as (
    select * from {{ ref('stg_payments') }}
),

pivoted as (
    select
        order_id,
    
        {% for payment_method in payment_methods -%}
            sum(if(payment_method = '{{ payment_method}}', 1, 0)) {{payment_method}}_amount
            {%- if not loop.last -%}, {%- endif %}
        {% endfor -%}

    from payments
    where status = 'success'
    group by 1
)

select * from pivoted