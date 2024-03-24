with
    fonte_businessentityaddress as (
        select *
        from {{ source('sap', 'businessentityaddress') }}
    )
    ,renomear as (
        select 
             cast(BUSINESSENTITYID as int) as fk_business
            ,cast(ADDRESSID as int) as fk_address
            ,cast(ADDRESSTYPEID as string) as fk_adresstype
            ,cast(ROWGUID as string) as fk_rowid
            --,cast(MODIFIEDDATE as date) as data_dados
        from fonte_businessentityaddress 
    )
select *
from renomear
       