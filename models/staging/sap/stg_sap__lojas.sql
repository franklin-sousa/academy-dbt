with 
    fonte_lojas as (
        select *
        from {{ source('sap', 'store') }}
    )
    ,renomear as (
        select 
        cast(BUSINESSENTITYID as int) as fk_business
        ,cast(SALESPERSONID as int) as fk_venda
        ,cast(ROWGUID as string) as fk_rowid
        ,cast(NAME as string) as nm_loja
        ,cast(DEMOGRAPHICS as string) as demografia
        ,cast(MODIFIEDDATE as date) as data_dados
        from fonte_lojas
    )

select *
from renomear