with
    fonte_personcreditcard as (
        select *
        from {{ source('sap', 'personcreditcard') }}
    )
    , renomear as (
        select 
             cast(BUSINESSENTITYID as int) as BUSINESSENTITY_ID
            ,cast(CREDITCARDID as int) as CREDITCARD_ID
            --,cast(MODIFIEDDATE  as date) as data_dados  
        from fonte_personcreditcard

    )
select *
from renomear