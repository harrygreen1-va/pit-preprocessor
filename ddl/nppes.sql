if object_id('nppes', 'u') is not null
 drop table nppes;

create table nppes(
	npi_key pit_key not null constraint xpknppes  primary key clustered,
	npi bigint null,

	last_updated_date datetime not null constraint df_nppes_last_updated_date  default (getdate()),
	last_updated_user varchar(128) not null constraint df_nppes_last_updated_user  default (suser_name()) ,
)