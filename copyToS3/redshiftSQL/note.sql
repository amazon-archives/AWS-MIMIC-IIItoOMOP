drop table if exists note;

-- big table

create table note
(
  note_id integer identity not null,
  person_id integer null,
  note_date date null,
  note_time varchar(10) null,
  note_type_concept_id integer not null,
  -- note_text text not null,
  -- note_text varchar(max) not null,
  provider_id integer null,
  visit_occurrence_id integer null,
  note_source_value varchar(50) null,
  note_text_uuid varchar(50) not null,
  note_text_lookup_location varchar(200) not null
);

-- nurses typically write \nursing notes", a summary
-- of events which occurred during their shift period. when the patient is dis-
-- charged from the hospital, the responsible physician dictates a summary of the
-- entire hospitalization period, known as the \discharge summary". these reports
-- are recorded in noteevents table

-- confusion: i mapped note_source_value from category; is it okay?
-- also, i mapped visit_occurrence_id from icustay_id.
-- but there are some notes for which there is no icustay_id (like
-- discharge summary). should i use hadm_id instead? i used only
-- icustay_id in the visit_occurrence table as well.



copy note(person_id, note_date, note_time, note_type_concept_id, provider_id, visit_occurrence_id, note_source_value, note_text_uuid, note_text_lookup_location)
from 's3://${bucket}/stage/note.csv' 
credentials 'aws_iam_role=${redshift_arn}'
gzip
dateformat 'auto'
delimiter ','
ignoreheader 1;




