select * 
    from {{ ref('stg_greenhouse__rejection_reason_tmp') }}