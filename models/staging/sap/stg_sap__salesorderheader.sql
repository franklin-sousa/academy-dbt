with
    fonte_salesorderheader as (
        select *
        from {{ source('sap', 'salesorderheader') }}
    )
    , renomear as (
        select 
             cast(SALESORDERID as int) as SALESORDER_ID
            ,cast(CUSTOMERID as int) as CUSTOMER_ID
            ,cast(SALESPERSONID as int) as SALESPERSON_ID
            ,cast(TERRITORYID as int) as TERRITORY_ID
            ,cast(BILLTOADDRESSID as int) as BILLTOADDRESS_ID
            ,cast(SHIPTOADDRESSID as int) as SHIPTOADDRESS_ID
            ,cast(SHIPMETHODID as int) as SHIPMETHOD_ID
            ,cast(CREDITCARDID as int) as CREDITCARD_ID   
            ,cast(CURRENCYRATEID as int) as CURRENCYRATE_ID                     
            ,cast(REVISIONNUMBER as int) as NUMERO_REVISAO_ORDEM
            ,cast(ORDERDATE as date) as DATA_ORDEM
            ,cast(DUEDATE as date) as DATA_VENCIMENTO
            ,cast(SHIPDATE as date) as DATA_ENVIO
            ,cast(STATUS as string) as STATUS
            ,cast(ONLINEORDERFLAG as string) as ONLINE_ORDEM_FLAG
            ,cast(PURCHASEORDERNUMBER as string) as NUMERO_PEDICO_COMPRA
            ,cast(ACCOUNTNUMBER as string) as NUMERO_CONTA 
            ,cast(CREDITCARDAPPROVALCODE as string) as COD_APROVACAO_CARTAO_CREDITO
            ,cast(SUBTOTAL as decimal(10,2)) as SUBTOTAL
            ,round(cast(TAXAMT as decimal(10,4)),2) as IMPOSTO
            ,round(cast(FREIGHT as decimal(10,4)),2) as FRETE
            ,round(cast(TOTALDUE as decimal(10,4)),2) as TOTAL_DEVIDO 
            --,cast(ROWGUID as string) as ROW_ID
            --,cast(MODIFIEDDATE as date) as data_dados
        from fonte_salesorderheader
    )
select *
from renomear   
where 1=1
--and  SALESORDER_ID=43659