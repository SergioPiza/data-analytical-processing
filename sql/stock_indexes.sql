drop table if exists stock_indexes;

create table stock_indexes (
	id SERIAL primary key,
	codigo_acao_empresa varchar(10) not null,
	nome_empresa varchar(20),
	setor_codigo_empresa varchar(10),
	setor_empresa varchar(100),
	segmento_empresa varchar(100)
	
	--,constraint un_codigo_acao_empresa UNIQUE(codigo_acao_empresa)	
);