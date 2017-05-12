--NOTE -- TABLE STRUCTURE AND SQL PROCESSING QUERY COPIED FROM https://github.com/shamsbayzid/mimic-cdm/blob/master/SQL-mimic-cdm.txt
--      mappings may or may not be accurate

drop table if exists measurement;

create table measurement
(
  measurement_id integer identity not  null primary key,
  person_id integer not null, 
  measurement_concept_id integer not null,
  measurement_date date not null,
  measurement_time varchar(10) null,
  measurement_type_concept_id integer not null,
  operator_concept_id integer null,
  --value_as_number numeric null,
  value_as_number varchar(100) null,
  value_as_concept_id integer null,
  unit_concept_id integer null,
  range_low numeric null,
  range_high numeric null,
  provider_id integer null,
  visit_occurrence_id integer null,
  measurement_source_value varchar(50) null,
  measurement_source_concept_id integer null,
  unit_source_value varchar(50) null,
  value_source_value varchar(50) null
);


copy measurement (person_id , 
  measurement_concept_id ,
  measurement_date ,
  measurement_time ,
  measurement_type_concept_id ,
  value_as_number ,
  visit_occurrence_id ,
  measurement_source_value ,
  unit_source_value 
   )
from 's3://${bucket}/stage/measurement.csv' 
credentials 'aws_iam_role=${redshift_arn}'
gzip
dateformat 'auto'
delimiter ','
ignoreheader 1;
