with source as (

  select * from {{ ref('day_2_data') }}

),

split as (

  select
    passwords as original,
    cast(
      REGEXP_EXTRACT(passwords, r"^[0-9]+")
    as INT64) as first_position,
    cast(
      REGEXP_EXTRACT(
        REGEXP_EXTRACT(passwords, r"-\d+"), r"\d+"
      )
    as INT64) as second_position,
    REGEXP_EXTRACT(passwords, r"[a-zA-Z]") as letter,
    REGEXP_EXTRACT(passwords, r"[a-zA-Z]+$") as password,
  from
    source

),

num_letters_in_pw as (

  select
    count(password) as num_valid
  from
    split
  where
    if(SUBSTR(password, first_position, 1) = letter, 1, 0) + if(SUBSTR(password, second_position, 1) = letter, 1, 0) = 1

)

select * from num_letters_in_pw
