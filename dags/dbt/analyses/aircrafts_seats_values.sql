select distinct
    fare_conditions
from
    {{ ref('stg_flights__seats') }}