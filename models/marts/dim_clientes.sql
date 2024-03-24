with 

    ,pessoas as (
        select 
             FK_BUSINESS
            ,FK_ROWID
            ,TIPO_PESSOA
            ,PRONONE_TRATAMENTO
            ,PRIMEIRO_NOME
            ,NOME_DO_MEIO
            ,SOBRE_NOME
            ,SUFIXO
        from {{ ref('stg_sap__pessoas') }}
    )
select *
from pessoas
     inner join clientes on pessoas.FK_ROWID=clientes.FK_ROWID