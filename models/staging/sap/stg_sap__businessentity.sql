with
    fonte_businessentity as (
        select *
        from {{ source('sap', 'businessentity') }}
    )
    , renomear as (
        select 
            cast(BUSINESSENTITYID as int) as fk_business
            ,cast(ROWGUID as string) as fk_rowid
            --,cast(MODIFIEDDATE as date) as data_dados
        from fonte_businessentity
    )
select *
from renomear    
