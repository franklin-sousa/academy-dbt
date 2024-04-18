with
    fonte_credito_pessoal as (
        select
            *
        from {{ source('sap', 'personcreditcard') }}
    )
, renomear as (
    select 
         cast(BUSINESSENTITYID as int) as pf_business
        ,cast(CREDITCARDID as int) as fk_credito
        ,cast(MODIFIEDDATE as date) as data_dados
     
from fonte_credito_pessoal
)

select *
from renomear
