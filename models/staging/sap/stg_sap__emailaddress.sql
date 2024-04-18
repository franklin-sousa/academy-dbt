with 
    fonte_emailaddress as (
        select *
        from {{ source('sap', 'emailaddress') }}
    )
    , renomear as (
        select 
             cast(EMAILADDRESSID as int) as pk_email
            ,cast(BUSINESSENTITYID as int) as fk_business
            ,cast(ROWGUID as string) as fk_rowid
            ,cast(EMAILADDRESS as string) as email
            --,cast(MODIFIEDDATE as string) as data_dados
        from fonte_emailaddress
    )
select * 
from renomear    