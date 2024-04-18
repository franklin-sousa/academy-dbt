with
    fonte_creditcard as (
        select *
        from {{ source('sap', 'creditcard') }}
    )
    , renomear as (
        select 
             cast(CREDITCARDID as int) as CREDITCARD_ID
            ,cast(CARDTYPE as string) as tipo_cartao
            ,cast(CARDNUMBER as string) as numero_cartao
            ,cast(EXPMONTH as int) as mes_validade_cartao
            ,cast(EXPYEAR as int) as ano_validade_cartao
            --,cast(MODIFIEDDATE as date) as 
        from fonte_creditcard
    )
select *
from renomear  