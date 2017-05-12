--NOTE -- TABLE STRUCTURE AND SQL PROCESSING QUERY COPIED FROM https://github.com/shamsbayzid/mimic-cdm/blob/master/SQL-mimic-cdm.txt
--      mappings may or may not be accurate

drop table if exists specimen;

create table specimen
( 
   specimen_id integer identity not null primary key ,
	 person_id integer not null ,
	 specimen_concept_id integer not null ,
	 specimen_type_concept_id integer not null ,
	 specimen_date date not null ,
	 specimen_time varchar(10) null ,
	 quantity numeric null ,
	 unit_concept_id integer null ,
	 anatomic_site_concept_id integer null ,
	 disease_status_concept_id integer null ,
	 specimen_source_id varchar(50) null ,
	 specimen_source_value varchar(50) null ,
	 unit_source_value varchar(50) null ,
	 anatomic_site_source_value varchar(50) null ,
	 disease_status_source_value varchar(50) null
);

-- it seems to be a subset of measurement table
-- confusion: specimen_source_id is the itemid or loinc_code? i have used loinc_code
-- how about specimen_source_value? i have used the name of the fluid.

copy specimen(person_id, specimen_concept_id, specimen_type_concept_id,specimen_date, 
specimen_time, unit_concept_id, anatomic_site_concept_id, disease_status_concept_id, 
specimen_source_id, specimen_source_value, unit_source_value)
from 's3://${bucket}/stage/specimen.csv' 
credentials 'aws_iam_role=${redshift_arn}'
dateformat 'auto'
gzip
delimiter ','
ignoreheader 1;
