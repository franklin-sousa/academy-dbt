with
    fonte_pessoas as (
        select
            *
        from {{ source('sap', 'person') }}
    )
    , renomear as (
        select 
             cast(BUSINESSENTITYID as string) as fk_business
            , cast(PERSONTYPE as string) as tipo_pessoa
            , cast(NAMESTYLE as string) as estilo_nome
            , cast(TITLE as string) as pronone_tratamento
            , cast(FIRSTNAME as string) as primeiro_nome
            , cast(MIDDLENAME as string) as nome_do_meio
            , cast(LASTNAME as string) as sobre_nome
            , cast(SUFFIX as string) as sufixo
            --, cast(EMAILPROMOTION as string) as 
            --, cast(ADDITIONALCONTACTINFO as string) as 
            --, cast(DEMOGRAPHICS as string) as 
            --, cast(ROWGUID as string) as 
            --, cast(MODIFIEDDATE as string) as 
        from fonte_pessoas

    )
select *
from renomear