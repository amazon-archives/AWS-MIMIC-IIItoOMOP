--NOTE -- TABLE STRUCTURE AND SQL PROCESSING QUERY COPIED FROM https://github.com/shamsbayzid/mimic-cdm/blob/master/SQL-mimic-cdm.txt
--      mappings may or may not be accurate

drop table if exists drug_exposure;

create table drug_exposure
(
  drug_exposure_id integer identity not null ,
  person_id integer not null ,
  drug_concept_id integer not null ,
  drug_exposure_start_date date not null ,
  drug_exposure_end_date date null ,
  drug_type_concept_id integer not null ,
  stop_reason varchar(20) null ,
  refills integer null ,
  quantity numeric null ,
  days_supply integer null ,
  sig text null ,
  route_concept_id integer null ,
  effective_drug_dose numeric null ,
  dose_unit_concept_id integer null ,
  lot_number varchar(50) null ,
  provider_id integer null ,
  visit_occurrence_id integer null ,
  drug_source_value varchar(50) null ,
  drug_source_concept_id integer null ,
  route_source_value varchar(50) null ,
  dose_unit_source_value varchar(50) null
);

-- confusion: drug_source_value: is it itemid or the label?

copy drug_exposure
 (
    person_id
    , drug_concept_id
    , drug_exposure_start_date
    , drug_type_concept_id
    , effective_drug_dose
    , provider_id
    , visit_occurrence_id
    , drug_source_value
    , drug_source_concept_id
    , route_source_value
    , dose_unit_source_value
  )
from 's3://${bucket}/stage/drug-exposure.csv' 
credentials 'aws_iam_role=${redshift_arn}'
gzip
dateformat 'auto'
delimiter ','
ignoreheader 1;
