--NOTE -- TABLE STRUCTURE AND SQL PROCESSING QUERY COPIED FROM https://github.com/shamsbayzid/mimic-cdm/blob/master/SQL-mimic-cdm.txt
--      mappings may or may not be accurate

drop table if exists visit_occurrence;

create table visit_occurrence 
( 
	visit_occurrence_id integer not null , 
	person_id integer not null , 
	visit_concept_id integer not null , 
	visit_start_date date not null , 
	visit_start_time varchar(10) null ,
	visit_end_date date null ,
	visit_end_time varchar(10) null , 
	visit_type_concept_id integer null ,
	provider_id integer null,
	care_site_id integer null, 
	visit_source_value varchar(50) null,
	visit_source_concept_id integer null
);

-- i used icustay_days table. i may need to use admissions table as well. 9203 is for emergency visit

-- confusion: icustay_detail and icustay_days have different number of records. which one to use?
-- confusion: should i use icustay_id as the visit_occurrence_id or just a serial number?

copy visit_occurrence(visit_occurrence_id, person_id, visit_concept_id, visit_start_date, visit_start_time, visit_end_date, visit_end_time, visit_type_concept_id, visit_source_concept_id)
from 's3://${bucket}/stage/visit-occurrence.csv' 
credentials 'aws_iam_role=${redshift_arn}'
dateformat 'auto'
gzip
delimiter ','
ignoreheader 1;
