create or replace table data_warehouse.dim_categorias as 
with despesas as (
select distinct
	'Despesas' as tipo
, 	categoria
, 	subcategoria
from
  	operational.despesas
), 
receita as (
select distinct
  	'Receita' as tipo
, 	categoria
, 	subcategoria
from
  	operational.receita
),
lazer as (
select distinct
  	'Receita' as tipo
, 	'uso_area_lazer' as categoria 
, 	nome as subcategoria
from
  	operational.lazer
),
union_categorias as (
select * from despesas
union all
select * from receita
union all
select * from lazer
)
select 
	row_number() over () as categoria_id
,	*
from 
	union_categorias
order by 
	tipo, categoria, subcategoria;
