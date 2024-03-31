with
    fonte_salesorderdetail as (
        select *
        from {{ source('sap', 'salesorderdetail') }}
    )
    ,renomear as (
        select 
             cast(SALESORDERID as int) as SALESORDER_ID
            ,cast(SALESORDERDETAILID as int) as SALESORDERDETAIL_ID
            ,cast(PRODUCTID as int) as PRODUCT_ID
            ,cast(SPECIALOFFERID as int) as SPECIALOFFER_ID            
            ,cast(CARRIERTRACKINGNUMBER as string) as numero_rastreamento
            ,cast(ORDERQTY as int) as qtd_pedido
            ,cast(UNITPRICE as decimal(10,2)) as preco_unitario
            ,cast(UNITPRICEDISCOUNT as decimal(10,2)) as desconto_unitario
            --,cast(ROWGUID as string) as ROWGUID
            --,cast(MODIFIEDDATE as date) as data_dados
        from fonte_salesorderdetail
    )
select *
from renomear 
where 1=1
--and  SALESORDER_ID=43659