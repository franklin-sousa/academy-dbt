with
    fonte_phonenumbertype as (
        select *
        from {{ source('sap', 'phonenumbertype') }}
    )
    ,renomear as (
        select 
            cast(PHONENUMBERTYPEID as int ) pk_tipo_telefone
            ,cast(NAME as string) as tipo_telefone
            --,cast(MODIFIEDDATE as date) as data_dados
        from fonte_phonenumbertype
    )
select *
from renomear  