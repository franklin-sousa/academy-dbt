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
    ,clientes as (
        select *
        from {{ ref('dim_clientes') }}
    )
   
    ,joined_tabelas as (
        select 
             ordens.SALESORDER_ID
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
            ,ordens_detalhes.PRECO_UNITARIO*ordens_detalhes.QTD_PEDIDO as total_produto
            
        from ordens 
            left join ordens_detalhes  on ordens.SALESORDER_ID=ordens_detalhes.SALESORDER_ID
            

    )
select 
    * 
    --sum(subtotal)
    --,sum(imposto)
    --,sum(frete)
    --,sum(total_devido)
from joined_tabelas


