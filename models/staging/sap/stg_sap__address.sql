with 
    fonte_address as (
        select *
        from {{ source('sap', 'address') }}
    )
    ,renomear as (
        select 
             cast(ADDRESSID as int) as pk_addressid
            ,cast(STATEPROVINCEID as int) as stateprovince_id
            ,cast(ROWGUID as string) as fk_rowid
            ,cast(ADDRESSLINE1 as string) as endereco_1
            ,cast(ADDRESSLINE2 as string) as endereco_2
            ,cast(CITY as string) as cidade  
            ,cast(POSTALCODE as string) cd_postal
            ,cast(SPATIALLOCATION as string) as localidade

        from fonte_address
    )

select *
from renomear    
