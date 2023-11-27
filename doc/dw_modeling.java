// https://dbdiagram.io/

table financeiro {
  financeiro_id int [primary key]
  calendario_id int [ref: > dim_calendario.calendario_id]
  categoria_id int [ref: > dim_categorias.categoria_id]
  valor float
}

table dim_calendario {
  calendario_id int [primary key]
  data date
  ano int
  mes_nome string
}

table dim_categorias {
  categoria_id int [primary key]
  tipo string
  categoria string
  subcategoria string
}

table dim_unidades {
  unidade_id int [primary key]
  financeiro_id int [primary key, ref: - financeiro.financeiro_id]
  edificio string
  apartamento int
  nome_condomino string
}

table dim_funcionarios {
  funcionario_id int [primary key]
  financeiro_id int [primary key, ref: - financeiro.financeiro_id]
  nome_funcionario string
}