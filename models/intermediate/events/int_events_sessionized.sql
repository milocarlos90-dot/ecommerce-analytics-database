with events as (

    select *
    from {{ ref('int_events_cleaned') }}

),

/* -----------------------------------------------------------
   1. Calculate previous event timestamp per customer
----------------------------------------------------------- */

session_gaps as (

    select
        *,
        lag(event_ts) over (
            partition by customer_id
            order by event_ts, event_id
        ) as previous_event_ts
    from events

),

/* -----------------------------------------------------------
   2. Detect new session boundaries
----------------------------------------------------------- */

session_flags as (

    select
        *,

        case
            when previous_event_ts is null then 1
            when timestamp_diff(event_ts, previous_event_ts, minute) > 30 then 1
            else 0
        end as new_session_flag

    from session_gaps

),

/* -----------------------------------------------------------
   3. Create running session number
----------------------------------------------------------- */

session_numbers as (

    select
        *,

        sum(new_session_flag) over (
            partition by customer_id
            order by event_ts, event_id
            rows between unbounded preceding and current row
        ) as customer_session_number

    from session_flags --- Running session counter per user ordered by event time.

),

/* -----------------------------------------------------------
   4. Generate analytics session id
----------------------------------------------------------- */

sessionized as (

    select
        *,
        {{ dbt_utils.generate_surrogate_key([
        'customer_id',
        'customer_session_number'
    ]) }} as customer_session_id

    from session_numbers

)

select *
from sessionized