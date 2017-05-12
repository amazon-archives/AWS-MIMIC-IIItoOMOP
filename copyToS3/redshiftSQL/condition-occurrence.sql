--NOTE -- TABLE STRUCTURE AND SQL PROCESSING QUERY COPIED FROM https://github.com/shamsbayzid/mimic-cdm/blob/master/SQL-mimic-cdm.txt
--      mappings may or may not be accurate

drop table if exists condition_occurrence;

create table condition_occurrence 
( 
  -- condition_occurrence_id		integer		not null , 

  condition_occurrence_id int identity not null primary key,  
  person_id integer not null, 
  condition_concept_id integer not null, 
  condition_start_date date null, 
  condition_end_date date null, 
  condition_type_concept_id integer not null, 
  stop_reason varchar(20) null, 
  provider_id integer null, 
  visit_occurrence_id integer null, 
  condition_source_value varchar(50) null,
  condition_source_concept_id	integer null
);

-- not sure how to get condition_start_date and end_date.
-- don't see how to obtain visit_occurrence_id (icustay_id was used originally in visit_occurrence, but here we have hadm_id)


copy condition_occurrence
from 's3://${bucket}/stage/condition-occurrence.csv' 
credentials 'aws_iam_role=${redshift_arn}'
gzip
delimiter ','
ignoreheader 1;

