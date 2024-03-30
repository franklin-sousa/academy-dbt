with
    fonte_salesorderheadersalesreason as (
        select *
        from {{ source('sap', 'salesreason') }}
    )
    ,renomear as (
        select 
            cast(SALESREASONID  as int) as SALESREASON_ID
            ,cast(NAME as string) as nm_motivo_venda
            ,cast(REASONTYPE as string) as motivo
        from fonte_salesorderheadersalesreason
    )
select *
from renomear    