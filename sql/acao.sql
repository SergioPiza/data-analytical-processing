drop table if exists acao;

create table acao (
	id SERIAL primary key,
	chave_tempo integer references date_desc(id),
	chave_empresa integer REFERENCES stock_indexes(id),
	abertura_acao numeric,
	fechamento_acao numeric,
	pico_acao numeric,
	baixa_acao numeric,
	volume_acao numeric,
	
	constraint un_acao UNIQUE(chave_tempo, chave_empresa)
);