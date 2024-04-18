with
    fonte_cartao_credito as (
        select
             cast(CREDITCARDID as int) as pk_cartao_credito
            ,cast(CARDTYPE as string) as bandeira
            ,cast(CARDNUMBER as int) as cartao_numero
            ,cast(EXPMONTH as int) as mes_validade
            ,cast(EXPYEAR as int) as ano_validade
            --,cast(MODIFIEDDATE as string) as data_dados
        from {{ source('sap', 'creditcard') }}
    )
, renomear as (
    select 
*
     
from fonte_cartao_credito
)

select *
from renomear