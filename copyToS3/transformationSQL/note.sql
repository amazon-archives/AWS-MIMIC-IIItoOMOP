select noteevents.SUBJECT_ID as person_id
 , noteevents.CHARTDATE  as note_date
 , date_format(noteevents.CHARTDATE, 'hh:mm:ss') as note_time
 , 0 as note_type_concept_id
 , regexp_replace(regexp_replace(regexp_replace(noteevents.TEXT, '\n', '  <linebreak> '), '\"', '`'), ',', '<comma>') as note_text
 , noteevents.CGID as provider_id
 , icustays.ICUSTAY_ID as visit_occurrence_id
 , noteevents.CATEGORY as note_source_value
from mimic3_noteevents noteevents  
	left join mimic3_icustays icustays on noteevents.HADM_ID = icustays.HADM_ID 
where cast(noteevents.SUBJECT_ID as double) is not null