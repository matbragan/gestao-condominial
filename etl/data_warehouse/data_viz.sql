create or replace table data_warehouse.desnormalizada as
select 
	fin.financeiro_id 
,	cal.`data`
,	cat.tipo 
,	cat.categoria 
,	cat.subcategoria 
,	fun.nome_funcionario 
,	uni.edificio 
,	uni.apartamento 
,	uni.nome_condomino 
,	fin.valor 
from 
	data_warehouse.financeiro fin
	join data_warehouse.dim_calendario cal on fin.calendario_id = cal.calendario_id 
	join data_warehouse.dim_categorias cat on fin.categoria_id = cat.categoria_id 
	left join data_warehouse.dim_funcionarios fun on fin.financeiro_id = fun.financeiro_id 
	left join data_warehouse.dim_unidades uni on fin.financeiro_id = uni.financeiro_id;
