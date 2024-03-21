with
    fonte_categoria_produtos as ( 
        select *
        from {{ source('sap', 'productcategory') }}
    )
    ,renomear as (
        select 
             cast(PRODUCTCATEGORYID as int) as pk_produto_categoria
            ,cast(NAME as string) as nome
            --,cast(ROWGUID as string) as guia_linha
            --,cast(MODIFIEDDATE as string) as dta_dados
        from fonte_categoria_produtos
    )
select *
from renomear  