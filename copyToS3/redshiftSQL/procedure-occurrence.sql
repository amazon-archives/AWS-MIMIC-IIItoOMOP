--NOTE -- TABLE STRUCTURE AND SQL PROCESSING QUERY COPIED FROM https://github.com/shamsbayzid/mimic-cdm/blob/master/SQL-mimic-cdm.txt
--      mappings may or may not be accurate

drop table if exists procedure_occurrence;

create table procedure_occurrence
(
  procedure_occurrence_id integer identity null primary key,
  person_id integer not null ,
  procedure_concept_id integer not null default(0) ,
--  procedure_date date not null default('1/1/1900') ,
  procedure_date date null default('1/1/1900') ,
  procedure_type_concept_id integer not  null default(0) ,
  modifier_concept_id integer null ,
  quantity integer null ,
  provider_id integer null ,
  visit_occurrence_id integer null ,
  procedure_source_value varchar(50) null ,
  procedure_source_concept_id integer null ,
  qualifier_source_value varchar(50) null
);

copy procedure_occurrence(person_id, procedure_date,procedure_source_value)
from 's3://${bucket}/stage/procedure-occurrence.csv' 
credentials 'aws_iam_role=${redshift_arn}'
dateformat 'auto'
gzip
delimiter ','
ignoreheader 1;
