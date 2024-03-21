with
    fonte_produtos as ( 
        select *
        from {{ source('sap', 'product') }}
    )
    , renomear as (
        select 
              cast(PRODUCTID as int) as pk_produto
            , cast(PRODUCTSUBCATEGORYID as int) as fk_produto_subcategoria
            , cast(PRODUCTMODELID as string) as fk_produto_modelo
            , cast(NAME as string) as nm_produto
            , cast(PRODUCTNUMBER as string) as cod_produto
            --, cast(MAKEFLAG as string) as makeflag
            --, cast(FINISHEDGOODSFLAG as string) as bandeira_produtos_acabados
            --, cast(COLOR as string) as cor
            --, cast(SAFETYSTOCKLEVEL as string) as nivel_estoque_seguranca
            --, cast(REORDERPOINT as string) as ponto_recomenda
            --, cast(STANDARDCOST as numeric(10,2)) as custo_padrao
            --, cast(LISTPRICE as string) as lista_de_preco
            --, cast(SIZE as string) as tamamnho
            --, cast(SIZEUNITMEASURECODE as string) as cod_medida_tamanho 
            --, cast(WEIGHTUNITMEASURECODE as string) as cod_medida_peso
            --, cast(WEIGHT as string) as peso
            --, cast(DAYSTOMANUFACTURE as string) as dias_para_fabricacao
            --, cast(PRODUCTLINE as string) as inha_produto
            --, cast(CLASS as string) as classe
            --, cast(STYLE as string) as estilo
            --, cast(SELLSTARTDATE as string) as dta_inicio_venda 
            --, cast(SELLENDDATE as string) as dta_venda
            --, cast(DISCONTINUEDDATE as string) as dta_discontinuacao
            --, cast(ROWGUID as string) as guid_de_linha
            --, cast(MODIFIEDDATE as date) as dta_dados
        from fonte_produtos
    )
select *
from renomear 