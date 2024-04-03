with 
    /*tabela com todos os clientes*/
    pessoas as(
        select 
             FK_BUSINESS as BUSINESSENTITY_ID
            ,PRIMEIRO_NOME
            ,case 
                when NOME_DO_MEIO = null then ' '
                else NOME_DO_MEIO 
            end as NOME_DO_MEIO
            ,case
                when SOBRE_NOME=null then ' '
                else SOBRE_NOME
             end as SOBRE_NOME
            ,SUFIXO
        from {{ ref('stg_sap__pessoas') }}
    )
    /*cartoes de crédito*/
    ,personcreditcard as (
        select 
             BUSINESSENTITY_ID
            ,CREDITCARD_ID
        from {{ ref('stg_sap__personcreditcard') }}
    )
    ,cartao_credito as (
        select 
             CREDITCARD_ID
            ,TIPO_CARTAO
            ,NUMERO_CARTAO
            ,MES_VALIDADE_CARTAO
            ,ANO_VALIDADE_CARTAO
        from {{ ref('stg_sap__creditcardid') }}
    )
    ,cartao as (
        select 
             personcreditcard.BUSINESSENTITY_ID
            ,personcreditcard.CREDITCARD_ID
            ,TIPO_CARTAO
            ,NUMERO_CARTAO
            ,MES_VALIDADE_CARTAO
            ,ANO_VALIDADE_CARTAO
        from personcreditcard
            inner join cartao_credito on personcreditcard.CREDITCARD_ID=cartao_credito.CREDITCARD_ID
    )
    /*email*/
    ,email as (
        select 
             PK_EMAIL as email_id
            ,FK_BUSINESS as BUSINESSENTITY_ID
            ,EMAIL
        from {{ ref('stg_sap__emailaddress') }}
    )
    /*telefone pessoal*/
    ,telefone_pessoal as (
        select 
             FK_BUSINESS as BUSINESSENTITY_ID
            ,TELEFONE
            ,FK_TIPO_TELEFONE as PHONENUMBERTYPE_ID
        from {{ ref('stg_sap__personphone') }}
    )
    ,telefone_pessoal_tipo as (
        select 
            PK_TIPO_TELEFONE as PHONENUMBERTYPE_ID
            ,case 
                when TIPO_TELEFONE='Cell' then 'celular'
                when TIPO_TELEFONE='Home' then 'residencial'
                when TIPO_TELEFONE='Work' then 'trabalho'
            end as tipo_telefone
        from {{ ref('stg_sap__phonenumbertype') }}
    )
    ,telefone as (
        select 
             BUSINESSENTITY_ID
            ,TELEFONE
            ,tipo_telefone
        from telefone_pessoal
            inner join telefone_pessoal_tipo on telefone_pessoal.PHONENUMBERTYPE_ID=telefone_pessoal_tipo.PHONENUMBERTYPE_ID
    )
    /*endereço*/
    ,base_endereco_entidade as (
        select 
             FK_BUSINESS as BUSINESSENTITY_ID
            ,FK_ADDRESS as ADDRESS_ID
            ,FK_ADRESSTYPE AS ADDRESSTYPE_ID
        from {{ ref('stg_sap__businessentityaddress') }}
    )
    ,base_endereco as (
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
    ,base_provincia as (
        select 
             STATEPROVINCE_ID
            ,TERRITORY_ID
            ,SIGLA_ESTADO
            ,SIGLA_PAIS as COUNTRYREGIONCODE
            ,NOME as nm_provincia
        from {{ ref('stg_sap__stateprovince') }}
    )
    ,base_pais as (
        select 
             CODIGO_PAIS as COUNTRYREGIONCODE
            ,NM_PAIS
        from {{ ref('stg_sap__countryregion') }}
    )

    ,endereco as (
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

    /*join das tabelas*/
    ,join_tabelas as (
        select 
             pessoas.BUSINESSENTITY_ID
            ,cartao.CREDITCARD_ID
            ,pessoas.PRIMEIRO_NOME
            ,pessoas.NOME_DO_MEIO
            ,pessoas.SOBRE_NOME
            ,concat(pessoas.PRIMEIRO_NOME,' ',pessoas.SOBRE_NOME) as nm_cliente
            --,pessoas.SUFIXO
            ,cartao.TIPO_CARTAO
            ,cartao.NUMERO_CARTAO
            ,cartao.MES_VALIDADE_CARTAO
            ,cartao.ANO_VALIDADE_CARTAO
            ,email.email
            ,case when telefone.tipo_telefone='trabalho' then telefone.telefone end as telefone_trabalho
            ,case when telefone.tipo_telefone='residencial' then telefone.telefone end as telefone_residencial
            ,case when telefone.tipo_telefone='celular' then telefone.telefone end as telefone_celular
            ,endereco.endereco_1
            ,endereco.endereco_2
            ,endereco.nm_cidade
            ,endereco.nm_provincia
            ,endereco.sigla_provincia
            ,endereco.nm_PAIS
        from pessoas 
            left join cartao on  pessoas.BUSINESSENTITY_ID=cartao.BUSINESSENTITY_ID
            left join email on pessoas.BUSINESSENTITY_ID=email.BUSINESSENTITY_ID
            left join telefone on pessoas.BUSINESSENTITY_ID=telefone.BUSINESSENTITY_ID
            left join endereco on pessoas.BUSINESSENTITY_ID=endereco.BUSINESSENTITY_ID
            
    )

select 
    *
from join_tabelas   
where 1=1
--and BUSINESSENTITY_ID=1045
--and CREDITCARD_ID=16281

