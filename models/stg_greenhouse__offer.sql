with max_offer_version as (    
    select 
            distinct application_id
        ,   max(version)                                        as max_version
    from {{ ref('stg_greenhouse__offer_tmp') }}
    group by 1
),

part1 AS (
    select
            off.*
        ,   case when
                maxv.max_version = off.version then 'Max Version'
                else                                'Other' 
                end                                             as max_offer_version
    from {{ ref('stg_greenhouse__offer_tmp') }} off
        left join 
            max_offer_version maxv on maxv.application_id = off.application_id
    where maxv.max_version = off.version
),

max_offer_date AS( 
    select 
            distinct part1.application_id
        ,   max(part1.UPDATED_AT)                               as max_date
    from part1
    group by 1
),

final AS (
    select 
        part1.*
    from part1
    left join max_offer_date maxd on maxd.application_id = part1.application_id
    where maxd.max_date = part1.UPDATED_AT
)

select * from final