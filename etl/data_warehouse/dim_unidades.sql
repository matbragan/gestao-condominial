create or replace table data_warehouse.dim_unidades as
with unidades as (
select 
	row_number() over () as unidade_id
,	edificio 
,	apartamento 
,	nome as nome_condomino
from
	operational.moradores
where 
	tipo = 'Proprietário'
order by 
	edificio, apartamento 
)
select 
	uni.unidade_id
,	fin.financeiro_id 
,	uni.edificio
,	uni.apartamento
,	uni.nome_condomino
from
	data_warehouse.financeiro fin
	join data_warehouse.dim_categorias cat 
		on fin.categoria_id = cat.categoria_id
	join unidades uni
		on cat.subcategoria = concat(uni.apartamento, ' ', uni.edificio);
