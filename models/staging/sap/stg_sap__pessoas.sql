with
    fonte_pessoas as (
        select
            *
        from {{ source('sap', 'person') }}
    )
    , renomear as (
        select 
             cast(BUSINESSENTITYID as int) as fk_business
            , cast(ROWGUID as string) as fk_rowid
            --, cast(PERSONTYPE as string) as tipo_pessoa
            --, cast(NAMESTYLE as string) as estilo_nome
            --, cast(TITLE as string) as pronone_tratamento
            , cast(FIRSTNAME as string) as primeiro_nome
            , cast(MIDDLENAME as string) as nome_do_meio
            , cast(LASTNAME as string) as sobre_nome
            , cast(SUFFIX as string) as sufixo
            --, cast(EMAILPROMOTION as string) as 
            --, cast(ADDITIONALCONTACTINFO as string) as contato_adicional
            --, cast(DEMOGRAPHICS as string) as deografia
            --, cast(MODIFIEDDATE as string) as data_dados
        from fonte_pessoas

    )
select *
from renomear