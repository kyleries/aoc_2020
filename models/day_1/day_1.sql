with source as (

  select * from {{ ref('day_1_data') }}

),

final as (

  select
    distinct(first.expense_amount * second.expense_amount * third.expense_amount) as product
  from
    source first,
    source second,
    source third
  where
    first.expense_amount + second.expense_amount + third.expense_amount = 2020

)

select * from final
