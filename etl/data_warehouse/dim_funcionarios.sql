create or replace table data_warehouse.dim_funcionarios as
with funcionarios as (
select 
	row_number() over () as funcionario_id
,	nome as nome_funcionario
from
	operational.funcionarios 
)
select 
	fun.funcionario_id
,	fin.financeiro_id 
,	fun.nome_funcionario
from
	data_warehouse.financeiro fin
	join data_warehouse.dim_categorias cat 
		on fin.categoria_id = cat.categoria_id
	join funcionarios fun
		on cat.subcategoria like concat('%', fun.nome_funcionario, '%');
