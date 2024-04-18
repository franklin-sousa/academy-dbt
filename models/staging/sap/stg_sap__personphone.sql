with 
    fonte_personphone as (
        select *
        from {{ source('sap', 'personphone') }}
    )
    , renomear as (
        select 
            cast(BUSINESSENTITYID as int) as fk_business
            ,cast(PHONENUMBER as string) as telefone
            ,cast(PHONENUMBERTYPEID as int) as fk_tipo_telefone
            --,cast(MODIFIEDDATE as string) as data_dados
        from fonte_personphone
    )

    select *
    from renomear