--create table data_warehouse.financeiro as 
create table data_warehouse.financeiro as 
with despesas as (
select distinct
	'Despesas' as tipo
, 	categoria
, 	subcategoria
,	mes as `data`
,	valor 
from
  	operational.despesas
), 
receita as (
select distinct
  	'Receita' as tipo
, 	categoria
, 	subcategoria
,	mes as `data`
,	valor 
from
  	operational.receita
),
lazer as (
select distinct
  	'Receita' as tipo
, 	'uso_area_lazer' as categoria 
, 	nome as subcategoria
,	data_uso as `data`
,	valor_pago as valor
from
  	operational.lazer
),
union_financeiro as (
select * from despesas
union all
select * from receita
union all
select * from lazer
)
select 
	row_number() over () as financeiro_id
,	cal.calendario_id 
,	cat.categoria_id 
,	fin.valor
from 
	union_financeiro fin
	join data_warehouse.dim_calendario cal
		on fin.`data` = cal.`data` 
	join data_warehouse.dim_categorias cat
		on fin.tipo = cat.tipo and fin.categoria = cat.categoria and fin.subcategoria = cat.subcategoria
