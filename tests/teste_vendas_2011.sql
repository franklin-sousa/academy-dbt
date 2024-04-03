/*sr. Carlos solicitou o teste de vendas brutas no ano de 2011 foram de $12.646.112,16*/

with
    vendas_em_2011 as (
        select 
            sum(total_produto)/1000 as faturamento
        from {{ ref('fato_vendas') }}
        where 1=1
        and data_ordem between '2011-01-01' and '2011-12-31'
        
    )
select 
    faturamento
from vendas_em_2011  
where faturamento not between 12646 and 12647
