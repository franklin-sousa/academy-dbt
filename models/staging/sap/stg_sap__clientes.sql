with
    fonte_clientes as (
        select
            *
        from {{ source('sap', 'customer') }}
    )
,renomear as (
    select
     cast(CUSTOMERID as int) as pk_clientes 
    ,cast(PERSONID as int) as  fk_pessoas
    ,cast(STOREID as int) as fk_stored
    ,cast(TERRITORYID as int) as fk_territorio 
    ,cast(ROWGUID as string) as fk_rowid
    --,cast(MODIFIEDDATE as string) as data_dados
from fonte_clientes
)
select * 
from renomear
