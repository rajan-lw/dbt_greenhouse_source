with base as (

    select * 
    from {{ ref('stg_greenhouse__offer_tmp') }}

),

max_offer_version as (

    select distinct off.application_id, max(off.version) as max_version
    from base off
    group by 1

),

final as (

    select 

    off.*
    ,CASE WHEN maxv.max_version=off.version THEN 'Max Version'
        ELSE 'Other' END AS max_offer_version

    from base off

    left join max_offer_version maxv on maxv.application_id=off.application_id

    where maxv.max_version=off.version


)

select * from final