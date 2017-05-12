--NOTE -- TABLE STRUCTURE AND SQL PROCESSING QUERY COPIED FROM https://github.com/shamsbayzid/mimic-cdm/blob/master/SQL-mimic-cdm.txt
--      mappings may or may not be accurate
drop table if exists person;

CREATE TABLE person 
(
     person_id int not null, 
     gender_concept_id	int not null, 
     year_of_birth	int not null, 
     month_of_birth	int null, 
     day_of_birth	int null, 
	   time_of_birth	character varying null,
     race_concept_id	int not null, 
     ethnicity_concept_id	int not null, 
     location_id	int null, 
     provider_id	int null, 
     care_site_id	int null, 
     person_source_value	character varying null, 
     gender_source_value character varying null,
	   gender_source_concept_id int null, 
     race_source_value	character varying null, 
	   race_source_concept_id int null, 
     ethnicity_source_value	character varying null,
	 ethnicity_source_concept_id int null
);

copy person(person_id, gender_concept_id, year_of_birth, month_of_birth,day_of_birth,race_concept_id, ethnicity_concept_id, gender_source_value)
from 's3://${bucket}/stage/person.csv' 
credentials 'aws_iam_role=${redshift_arn}'
gzip
delimiter ','
ignoreheader 1;
