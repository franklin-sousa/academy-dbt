with
    fonte_categoria_produtos as ( 
        select *
        from {{ source('sap', 'productsubcategory') }}
    )
    ,renomear as (
        select 
             cast(PRODUCTSUBCATEGORYID as int) as pk_produto_subcategoria
            ,cast(PRODUCTCATEGORYID as int) as fk_produto_categoria
            ,cast(NAME as string) as nm_produto_subcategoria
            --,cast(ROWGUID as string) as guia_linha
           --,cast(MODIFIEDDATE as date) as dta_dados
        from fonte_categoria_produtos
    )
select *
from renomear  