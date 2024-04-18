with
    fonte_stateprovince as (
        select *
        from {{ source('sap', 'stateprovince') }}
    )
    , renomear as (
        select 
             cast(STATEPROVINCEID as int)  as STATEPROVINCE_ID
            ,cast(TERRITORYID as string) as TERRITORY_ID   
            ,cast(ROWGUID as string) as fk_ROWGUID         
            ,cast(STATEPROVINCECODE as string) as sigla_estado
            ,cast(COUNTRYREGIONCODE as string) as sigla_pais
            --,cast(ISONLYSTATEPROVINCEFLAG as string) as 
            ,cast(NAME as string) as nome            
            --,cast(MODIFIEDDATE as date) as data_dados
        from fonte_stateprovince
    )
select *
from renomear    