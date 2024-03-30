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
        select 
             BUSINESSENTITY_ID
            ,CREDITCARD_ID
            ,PRIMEIRO_NOME
            ,NOME_DO_MEIO
            ,SOBRE_NOME
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
    ,produtos as (
        select 
             PK_PRODUTO as PRODUCT_ID
            ,NM_PRODUTO
            ,COD_PRODUTO
            ,NM_PRODUTO_SUBCATEGORIA
            ,NM_PRODUTO_CATEGORIA
        from {{ ref('dim_produtos') }}
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
            ,produtos.NM_PRODUTO
            ,produtos.COD_PRODUTO
            ,produtos.NM_PRODUTO_SUBCATEGORIA
            ,produtos.NM_PRODUTO_CATEGORIA
            ,ordens_detalhes.QTD_PEDIDO
            ,ordens_detalhes.PRECO_UNITARIO
            ,ordens_detalhes.DESCONTO_UNITARIO
            ,ordens_detalhes.PRECO_UNITARIO*ordens_detalhes.QTD_PEDIDO as total_produto
            ,clientes.PRIMEIRO_NOME
            ,clientes.NOME_DO_MEIO
            ,clientes.SOBRE_NOME
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
            

    )
select 
    * 
    --sum(subtotal)
    --,sum(imposto)
    --,sum(frete)
    --,sum(total_devido)
from joined_tabelas
where 1=1
and SALESORDER_ID=43659


