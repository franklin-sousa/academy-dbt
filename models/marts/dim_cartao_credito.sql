with
/*cartoes de crédito*/
    personcreditcard as (
        select 
             BUSINESSENTITY_ID
            ,CREDITCARD_ID
        from {{ ref('stg_sap__personcreditcard') }}
    )
    , cartao_credito as (
        select 
             CREDITCARD_ID
            ,TIPO_CARTAO
            ,NUMERO_CARTAO
            ,MES_VALIDADE_CARTAO
            ,ANO_VALIDADE_CARTAO
        from {{ ref('stg_sap__creditcardid') }}
    )
    , join_tabelas as (
        select 
             personcreditcard.BUSINESSENTITY_ID
            ,personcreditcard.CREDITCARD_ID
            ,TIPO_CARTAO
            ,NUMERO_CARTAO
            ,MES_VALIDADE_CARTAO
            ,ANO_VALIDADE_CARTAO
        from personcreditcard
            inner join cartao_credito on personcreditcard.CREDITCARD_ID=cartao_credito.CREDITCARD_ID
    )

select *
from join_tabelas
    