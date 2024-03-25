with
    ordens as (
        select 
             SALESORDER_ID
            ,CUSTOMER_ID
            ,SALESPERSON_ID
            ,TERRITORY_ID
            ,BILLTOADDRESS_ID
            ,SHIPTOADDRESS_ID
            ,SHIPMETHOD_ID
            ,CREDITCARD_ID
            ,CURRENCYRATE_ID
            ,NUMERO_REVISAO_ORDEM
            ,DATA_ORDEM
            ,DATA_VENCIMENTO
            ,DATA_ENVIO
            ,STATUS
            ,ONLINE_ORDEM_FLAG
            ,NUMERO_PEDICO_COMPRA
            ,NUMERO_CONTA
            ,COD_APROVACAO_CARTAO_CREDITO
            ,SUBTOTAL
            ,IMPOSTO
            ,FRETE
            ,TOTAL_DEVIDO
        from {{ ref('stg_sap__salesorderheader') }}
    )
    ,ordens_detalhes as (
        select
             SALESORDER_ID
            ,SALESORDERDETAIL_ID
            ,PRODUCT_ID
            ,SPECIALOFFER_ID
            ,NUMERO_RASTREAMENTO
            ,QTD_PEDIDO
            ,PRECO_UNITARIO
            ,DESCONTO_UNITARIO
        from {{ ref('stg_sap__salesorderdetail') }}
    )
    ,cartao_credito as (
        select 
             CREDITCARD_ID
            ,TIPO_CARTAO
            ,NUMERO_CARTAO
            ,MES_FIM_CARTAO
            ,ANO_FIM_CARTAO
        from {{ ref('stg_sap__creditcardid') }}
    ),
    cartao_cred_pessoal as (
        select 
             BUSINESSENTITY_ID
            ,CREDITCARD_ID
        from {{ ref('stg_sap__personcreditcard') }}
    )
    ,clientes as (
        select 
             FK_BUSINESS
            ,NOME
            ,EMAIL
            ,TEL_CELULAR
            ,TEL_TRABALHO
            ,TEL_CASA
            ,ENDERECO_1
            ,ENDERECO_2
            ,CIDADE
            ,CD_POSTAL
            ,NM_ESTADO
            ,SIGLA_PAIS
            ,NM_PAIS
        from {{ ref('dim_clientes') }}
    )
    ,joined_tabelas as (
        select 
             ordens.SALESORDER_ID
            ,cartao_cred_pessoal.BUSINESSENTITY_ID
            ,ordens.CUSTOMER_ID
            ,ordens.SALESPERSON_ID
            ,ordens.TERRITORY_ID
            ,ordens.BILLTOADDRESS_ID
            ,ordens.SHIPTOADDRESS_ID
            ,ordens.SHIPMETHOD_ID
            ,ordens.CREDITCARD_ID
            ,ordens.CURRENCYRATE_ID
            ,ordens.NUMERO_REVISAO_ORDEM
            ,ordens.DATA_ORDEM
            ,ordens.DATA_VENCIMENTO
            ,ordens.DATA_ENVIO
            ,ordens.STATUS
            ,ordens.ONLINE_ORDEM_FLAG
            ,ordens.NUMERO_PEDICO_COMPRA
            ,ordens.NUMERO_CONTA
            ,ordens.COD_APROVACAO_CARTAO_CREDITO
            ,ordens.SUBTOTAL
            ,ordens.IMPOSTO
            ,ordens.FRETE
            ,ordens.TOTAL_DEVIDO
            ,ordens_detalhes.NUMERO_RASTREAMENTO
            ,ordens_detalhes.QTD_PEDIDO
            ,ordens_detalhes.PRECO_UNITARIO
            ,ordens_detalhes.DESCONTO_UNITARIO
            ,clientes.NOME
            ,clientes.EMAIL
            ,clientes.TEL_CELULAR
            ,clientes.TEL_TRABALHO
            ,clientes.TEL_CASA
            ,clientes.ENDERECO_1
            ,clientes.ENDERECO_2
            ,clientes.CIDADE
            ,clientes.CD_POSTAL
            ,clientes.NM_ESTADO
            ,clientes.SIGLA_PAIS
            ,clientes.NM_PAIS
            ,cartao_credito.TIPO_CARTAO
            ,cartao_credito.NUMERO_CARTAO
            ,cartao_credito.MES_FIM_CARTAO
            ,cartao_credito.ANO_FIM_CARTAO
        from ordens_detalhes
            left join ordens on ordens.SALESORDER_ID=ordens_detalhes.SALESORDER_ID
            left join cartao_credito on ordens.CREDITCARD_ID=cartao_credito.CREDITCARD_ID
            left join cartao_cred_pessoal on cartao_cred_pessoal.CREDITCARD_ID = cartao_credito.CREDITCARD_ID
            left join clientes on clientes.FK_BUSINESS = cartao_cred_pessoal.BUSINESSENTITY_ID

    )
select *
from joined_tabelas

