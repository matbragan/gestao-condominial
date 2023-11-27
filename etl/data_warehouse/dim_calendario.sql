create or replace table data_warehouse.dim_calendario as
select
	row_number() over () as calendario_id
,  	`data`
, 	extract(year from `data`) as ano
, 	format_date('%B', `data`) as mes_nome
from
	unnest(generate_date_array(date '2023-01-01', date '2023-12-31', interval 1 day)) as `data`
order by 
	`data` asc;
