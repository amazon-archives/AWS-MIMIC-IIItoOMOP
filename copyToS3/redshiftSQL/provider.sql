--NOTE -- TABLE STRUCTURE AND SQL PROCESSING QUERY COPIED FROM https://github.com/shamsbayzid/mimic-cdm/blob/master/SQL-mimic-cdm.txt
--      mappings may or may not be accurate

drop table if exists provider;

create table provider
(
  provider_id integer not null,
  provider_name varchar(255) null,
  npi varchar(20) null,
  dea varchar(20) null,
  specialty_concept_id integer null,
  care_site_id integer null,
  year_of_birth integer null,
  gender_concept_id integer null,
  provider_source_value varchar(50) null,
  specialty_source_value varchar(50) null,
  specialty_source_concept_id integer null,
  gender_source_value varchar(50) null,
  gender_source_concept_id integer null
);

copy provider (provider_id, provider_source_value, specialty_concept_id, specialty_source_value, specialty_source_concept_id)
from 's3://${bucket}/stage/provider.csv' 
credentials 'aws_iam_role=${redshift_arn}'
gzip
delimiter ','
ignoreheader 1;
