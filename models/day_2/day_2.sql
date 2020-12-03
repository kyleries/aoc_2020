with source as (

  select * from {{ ref('day_2_data') }}

),

split as (

  select
    passwords as original,
    cast(
      REGEXP_EXTRACT(passwords, r"^[0-9]+")
    as INT64) as minimum,
    cast(
      REGEXP_EXTRACT(
        REGEXP_EXTRACT(passwords, r"-\d+"), r"\d+"
      )
    as INT64) as maximum,
    REGEXP_EXTRACT(passwords, r"[a-zA-Z]") as letter,
    REGEXP_EXTRACT(passwords, r"[a-zA-Z]+$") as password,
    concat("[", REGEXP_EXTRACT(passwords, r"[a-zA-Z]"), "]") as regex_to_count
  from
    source

),

num_letters_in_pw as (

  select
    count(password) as num_valid,
  from
    split
  where
        ARRAY_LENGTH(REGEXP_EXTRACT_ALL(password, regex_to_count)) >= minimum
    and ARRAY_LENGTH(REGEXP_EXTRACT_ALL(password, regex_to_count)) <= maximum

)

select * from num_letters_in_pw
