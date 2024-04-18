with 
/*localidade*/
    base_endereco_entidade as (
        select 
             FK_BUSINESS as BUSINESSENTITY_ID
            ,FK_ADDRESS as ADDRESS_ID
            ,FK_ADRESSTYPE AS ADDRESSTYPE_ID
        from {{ ref('stg_sap__businessentityaddress') }}
    )
    , base_endereco as (
        select 
             PK_ADDRESSID as ADDRESS_ID
            ,STATEPROVINCE_ID
            ,ENDERECO_1
            ,ENDERECO_2
            ,CIDADE
            ,CD_POSTAL
            ,LOCALIDADE
        from {{ ref('stg_sap__address') }}
    )
    , base_provincia as (
        select 
             STATEPROVINCE_ID
            ,TERRITORY_ID
            ,SIGLA_ESTADO
            ,SIGLA_PAIS as COUNTRYREGIONCODE
            ,NOME as nm_provincia
        from {{ ref('stg_sap__stateprovince') }}
    )
    , base_pais as (
        select 
             CODIGO_PAIS as COUNTRYREGIONCODE
            ,NM_PAIS
        from {{ ref('stg_sap__countryregion') }}
    )

    , join_tabelas as (
        select 
             base_endereco_entidade.BUSINESSENTITY_ID
            ,base_endereco.ENDERECO_1
            ,base_endereco.ENDERECO_2
            ,base_endereco.CIDADE as nm_cidade
            ,base_provincia.nm_provincia
            ,base_provincia.SIGLA_ESTADO as sigla_provincia
            ,base_pais.NM_PAIS
        from base_endereco_entidade
            left join base_endereco on base_endereco_entidade.ADDRESS_ID=base_endereco.ADDRESS_ID
            left join base_provincia on base_provincia.STATEPROVINCE_ID =base_endereco.STATEPROVINCE_ID
            left join base_pais on base_pais.COUNTRYREGIONCODE = base_provincia.COUNTRYREGIONCODE

    )

select *
from join_tabelas    