drop table if exists date_desc;

create table date_desc (
	id SERIAL primary key,
	data DATE not null,
	dia integer,
	mes integer,
	ano integer,
	mes_desc char(3),
	mes_desc_full varchar(10),
	bimestre integer,
	trimestre integer,
	semestre integer,
	dia_semana integer,
	dia_semana_desc char(3),
	dia_semana_desc_full varchar(10),
	
	constraint un_data UNIQUE(data),
	CONSTRAINT ck_dia CHECK (dia > 0 and dia < 32),
	CONSTRAINT ck_mes CHECK (mes > 0 and mes < 13),
	CONSTRAINT ck_mes_desc CHECK (mes_desc in ('JAN', 'FEV', 'MAR', 'ABR', 'MAI', 'JUN', 
											   'JUL', 'AGO', 'SET', 'OUT', 'NOV', 'DEZ')),
	CONSTRAINT ck_mes_desc_full CHECK (mes_desc_full in ('JANEIRO', 'FEVEREIRO', 'MARCO',
														 'ABRIL', 'MAIO', 'JUNHO', 
														 'JULHO', 'AGOSTO', 'SETEMBRO', 
														 'OUTUBRO', 'NOVEMBRO', 'DEZEMBRO')),
	CONSTRAINT ck_bimestre CHECK (bimestre > 0 and bimestre < 7),
	CONSTRAINT ck_trimestre CHECK (trimestre > 0 and trimestre < 5),
	CONSTRAINT ck_semestre CHECK (semestre > 0 and semestre < 3),
	CONSTRAINT ck_dia_semana CHECK (dia_semana > 0 and dia_semana < 8),
	CONSTRAINT ck_dia_semana_desc CHECK (dia_semana_desc in ('SEG', 'TER', 'QUA', 'QUI', 'SEX', 'SAB', 'DOM')),
	CONSTRAINT ck_dia_semana_desc_full CHECK (dia_semana_desc_full in ('SEGUNDA', 'TERCA', 'QUARTA',
																	   'QUINTA', 'SEXTA', 'SABADO', 'DOMINGO'))
	
);