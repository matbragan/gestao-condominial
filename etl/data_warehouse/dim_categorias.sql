create or replace table data_warehouse.dim_categorias as 
with despesas as (
select distinct
	'Despesas' as tipo
, 	categoria
, 	subcategoria
from
  	operational.despesas
), 
ganhos as (
select distinct
  	'Ganhos' as tipo
, 	categoria
, 	subcategoria
from
  	operational.ganhos
),
lazer as (
select distinct
  	'Ganhos' as tipo
, 	'USO AREA LAZER' as categoria 
, 	concat(apartamento, ' ', edificio) as subcategoria
from
  	operational.lazer
),
union_categorias as (
select * from despesas
union all
select * from ganhos
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
