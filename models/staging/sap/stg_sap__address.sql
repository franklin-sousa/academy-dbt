with 
    fonte_address as (
        select *
        from {{ source('sap', 'address') }}
    )
    ,renomear as (
        select 
             cast(ADDRESSID as int) as pk_addressid
            ,cast(ADDRESSLINE1 as string) as endereco_1
            ,cast(ADDRESSLINE2 as string) as endereco2
            ,cast(CITY as string) as cidade 
            ,cast(STATEPROVINCEID as int) as estado
            ,POSTALCODE as cd_postal
            --,cast(SPATIALLOCATION as string) as localidade
            --,cast(ROWGUID as string) as fk_rowid
        from fonte_address
    )

select *
from renomear    