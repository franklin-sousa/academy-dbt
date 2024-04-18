with
    fonte_salesorderheadersalesreason as (
        select *
        from {{ source('sap', 'salesorderheadersalesreason') }}
    )
    , renomear as (
        select 
             cast(SALESORDERID as int) as SALESORDER_ID
            ,cast(SALESREASONID as int) as SALESREASON_ID
            --,cast(MODIFIEDDATE as date) as data_dados
        from fonte_salesorderheadersalesreason
    )
select *
from renomear    