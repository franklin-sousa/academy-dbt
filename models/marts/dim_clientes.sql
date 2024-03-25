with 
    pessoas as (
        select 
             FK_BUSINESS
            ,FK_ROWID
            ,PRIMEIRO_NOME||' '||NOME_DO_MEIO||' '||SOBRE_NOME as nome
        from {{ ref('stg_sap__pessoas') }}
    )
    ,email as(
        select 
             PK_EMAIL
            ,FK_BUSINESS
            ,FK_ROWID
            ,EMAIL
        from {{ ref('stg_sap__emailaddress') }}
    )
    ,telefone as (
        select 
             FK_BUSINESS
            ,TELEFONE
            ,FK_TIPO_TELEFONE
        from {{ ref('stg_sap__personphone') }}
    )
    ,tipo_telefone as (
        select 
             PK_TIPO_TELEFONE
            ,TIPO_TELEFONE
        from {{ ref('stg_sap__phonenumbertype') }}
    )
    /*montando e classificando numero de telefone*/
    ,contato_telefone as(
        select 
             telefone.FK_BUSINESS
            ,telefone.TELEFONE
            ,case
                when tipo_telefone='Cell' then 'celular'
                when tipo_telefone='Home' then 'casa'
                when tipo_telefone='Work' then 'trabalho'
            end tp_telefone
        from telefone
            left join tipo_telefone on telefone.FK_TIPO_TELEFONE=PK_TIPO_TELEFONE
    )
    /*montando endereco*/
    ,endereco_id as (
        select 
             FK_BUSINESS
            ,FK_ROWID
        from {{ ref('stg_sap__businessentity') }}
    )
    ,endereco_negocio_id as(
        select 
             FK_BUSINESS
            ,FK_ADDRESS
            ,FK_ADRESSTYPE
            ,FK_ROWID
        from {{ ref('stg_sap__businessentityaddress') }}
    )
    ,endereco_cad as (
        select 
             PK_ADDRESSID
            ,stateprovince_id
            ,endereco_1
            ,endereco_2
            ,CIDADE
            ,CD_POSTAL
        from {{ ref('stg_sap__address') }}
    )
    ,sigla_estado as (
        select 
             STATEPROVINCE_ID
            ,TERRITORY_ID
            ,FK_ROWGUID
            ,SIGLA_ESTADO 
            ,SIGLA_PAIS
            ,NOME as nm_estado
        from {{ ref('stg_sap__stateprovince') }}
    )
    ,sigla_pais as (
        select 
            CODIGO_PAIS
            ,NM_PAIS
        from {{ ref('stg_sap__countryregion') }}
    )
    ,endereco as (
        select 
             endereco_id.FK_BUSINESS
            --,endereco_negocio_id.*
            ,endereco_cad.PK_ADDRESSID
            ,endereco_cad.stateprovince_id
            ,endereco_cad.ENDERECO_1 
            ,endereco_cad.ENDERECO_2
            ,endereco_cad.CIDADE
            ,endereco_cad.CD_POSTAL
            ,sigla_estado.nm_estado
            ,sigla_estado.SIGLA_PAIS
            ,NM_PAIS
        from endereco_id
            inner join endereco_negocio_id on endereco_id.FK_BUSINESS=endereco_negocio_id.FK_BUSINESS 
            inner join endereco_cad on endereco_cad.PK_ADDRESSID = endereco_negocio_id.FK_ADDRESS
            inner join sigla_estado on sigla_estado.stateprovince_id = endereco_cad.stateprovince_id
            inner join sigla_pais on sigla_pais.CODIGO_PAIS=sigla_estado.SIGLA_PAIS


    )
    /*juntado tabelas*/
    ,join_tabelas as (
        select 
             pessoas.FK_BUSINESS
            --,pessoas.FK_ROWID
            ,pessoas.nome
            ,email.email
            ,case when contato_telefone.tp_telefone='celular' then contato_telefone.TELEFONE end as tel_celular
            ,case when contato_telefone.tp_telefone='trabalho' then contato_telefone.telefone end tel_trabalho
            ,case when contato_telefone.tp_telefone='casa' then contato_telefone.telefone end tel_casa
            ,endereco.ENDERECO_1 
            ,endereco.ENDERECO_2
            ,endereco.CIDADE
            ,endereco.CD_POSTAL
            ,endereco.nm_estado
            ,endereco.SIGLA_PAIS
            ,endereco.NM_PAIS
        from pessoas
           left join email on pessoas.FK_BUSINESS=email.FK_BUSINESS
           left join contato_telefone on contato_telefone.FK_BUSINESS= pessoas.FK_BUSINESS
           left join endereco on endereco.FK_BUSINESS=pessoas.FK_BUSINESS
    )
select *
from join_tabelas 
