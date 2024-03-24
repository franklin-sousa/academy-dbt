with 
    fonte_countryregion as (
        select *
        from {{ source('sap', 'countryregion') }}
    )
    ,renomear as (
        select 
            cast(COUNTRYREGIONCODE as string) as codigo_pais
            ,cast(NAME as string) as nm_pais
            --,cast(MODIFIEDDATE as date) as data_dados
        from fonte_countryregion
    )
select *
from renomear    