with 
    produtos as (
        select   
              pk_produto
            , fk_produto_subcategoria
            , fk_produto_modelo
            , nm_produto
            , cod_produto
        from {{ ref('stg_sap__produtos') }}
    )
   ,subcategorias as (
        select 
             pk_produto_subcategoria
            ,fk_produto_categoria
            ,nm_produto_subcategoria
        from {{ ref('stg_sap__produtos_subcategoria') }}
    )
    ,categorias as (
        select 
             pk_produto_categoria
            ,nm_produto_categoria
        from {{ ref('stg_sap__produtos_categoria') }}
    )
    ,joined_subcat_categoria as (
        select 
             pk_produto_subcategoria
            ,nm_produto_subcategoria
            ,nm_produto_categoria
        from subcategorias
             left join categorias on categorias.pk_produto_categoria=subcategorias.fk_produto_categoria
    )
    ,joined_tabelas as (
        select 
              produtos.pk_produto
            --, produtos.fk_produto_subcategoria
            , produtos.fk_produto_modelo
            , produtos.nm_produto
            , produtos.cod_produto
            ,joined_subcat_categoria.nm_produto_subcategoria
            ,joined_subcat_categoria.nm_produto_categoria
        from produtos  
             left join joined_subcat_categoria on joined_subcat_categoria.pk_produto_subcategoria=produtos.fk_produto_subcategoria

    )

select *
from joined_tabelas
