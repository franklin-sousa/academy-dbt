with
    /*pedidos*/
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
    , ordens_detalhes as (
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
    /*motivo das vendas*/
    , base_motivo_vendas as (
        select 
             SALESORDER_ID
            ,SALESREASON_ID
        from {{ ref('stg_sap__salesorderheadersalesreason') }}

    )
    , base_tipo_motivo_vendas as (
        select 
             SALESREASON_ID
            ,NM_MOTIVO_VENDA
            ,MOTIVO
        from {{ ref('stg_sap__salesreason') }}
    )
    , motivo_venda as (
        select 
             base_motivo_vendas.SALESORDER_ID
            ,base_motivo_vendas.SALESREASON_ID
            ,base_tipo_motivo_vendas.NM_MOTIVO_VENDA
            ,base_tipo_motivo_vendas.MOTIVO
            ,row_number() OVER(partition by base_motivo_vendas.SALESORDER_ID ORDER BY base_motivo_vendas.SALESORDER_ID) AS ranking
        from base_motivo_vendas
            inner join base_tipo_motivo_vendas on base_motivo_vendas.SALESREASON_ID = base_tipo_motivo_vendas.SALESREASON_ID
    )
    /*clientes*/
    , clientes as (
        select 
             BUSINESSENTITY_ID
            ,CREDITCARD_ID
            ,NM_CLIENTE
            ,TIPO_CARTAO
            ,NUMERO_CARTAO
            ,MES_VALIDADE_CARTAO
            ,ANO_VALIDADE_CARTAO
            ,EMAIL
            ,TELEFONE_TRABALHO
            ,TELEFONE_RESIDENCIAL
            ,TELEFONE_CELULAR
            ,ENDERECO_1
            ,ENDERECO_2
            ,NM_CIDADE
            ,NM_PROVINCIA
            ,SIGLA_PROVINCIA
            ,NM_PAIS
        from {{ ref('dim_clientes2') }}
    )
    /*produtos*/
    , produtos as (
        select 
             PK_PRODUTO as PRODUCT_ID
            ,NM_PRODUTO
            ,COD_PRODUTO
            ,NM_PRODUTO_SUBCATEGORIA
            ,NM_PRODUTO_CATEGORIA
        from {{ ref('dim_produtos') }}
    )
   
    , joined_tabelas as (
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
            ,extract(year from ordens.DATA_ORDEM)||lpad(extract(month from ordens.DATA_ORDEM),2,'0') as ano_mes_dta_ordem
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
            ,ordens_detalhes.QTD_PEDIDO
            ,ordens_detalhes.PRECO_UNITARIO
            ,ordens_detalhes.DESCONTO_UNITARIO
            ,ordens_detalhes.PRECO_UNITARIO*ordens_detalhes.QTD_PEDIDO as total_produto
            ,round(
                ((ordens_detalhes.PRECO_UNITARIO*ordens_detalhes.QTD_PEDIDO)
                    -(ordens_detalhes.DESCONTO_UNITARIO*ordens_detalhes.QTD_PEDIDO))
                    +(ordens_detalhes.QTD_PEDIDO*(ordens.FRETE/sum(ordens_detalhes.QTD_PEDIDO) over (partition by ordens.SALESORDER_ID)))
                    +(ordens_detalhes.QTD_PEDIDO*(ordens.IMPOSTO/sum(ordens_detalhes.QTD_PEDIDO) over (partition by ordens.SALESORDER_ID)))
                ,2)   as total_bruto
            ,ordens_detalhes.NUMERO_RASTREAMENTO
            ,produtos.NM_PRODUTO
            ,produtos.COD_PRODUTO
            ,produtos.NM_PRODUTO_SUBCATEGORIA
            ,produtos.NM_PRODUTO_CATEGORIA
            ,motivo_venda.nm_motivo_venda
            ,clientes.NM_CLIENTE
            ,clientes.TIPO_CARTAO
            ,clientes.NUMERO_CARTAO
            ,clientes.MES_VALIDADE_CARTAO
            ,clientes.ANO_VALIDADE_CARTAO
            ,clientes.EMAIL
            ,clientes.TELEFONE_TRABALHO
            ,clientes.TELEFONE_RESIDENCIAL
            ,clientes.TELEFONE_CELULAR
            ,clientes.ENDERECO_1
            ,clientes.ENDERECO_2
            ,clientes.NM_CIDADE
            ,clientes.NM_PROVINCIA
            ,clientes.SIGLA_PROVINCIA
            ,clientes.NM_PAIS
            
            
        from ordens 
            left join ordens_detalhes  on ordens.SALESORDER_ID=ordens_detalhes.SALESORDER_ID
            left join clientes on clientes.CREDITCARD_ID=ordens.CREDITCARD_ID
            left join produtos on produtos.PRODUCT_ID=ordens_detalhes.PRODUCT_ID
            left join motivo_venda on ordens.SALESORDER_ID=motivo_venda.SALESORDER_ID and ranking=1
            

    )
select 
    * 
from joined_tabelas
where 1=1


