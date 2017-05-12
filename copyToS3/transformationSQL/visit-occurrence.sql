select ICUSTAY_ID as visit_occurrence_id
	, SUBJECT_ID as person_id 
	, 9203 as visit_concept_id 
	, INTIME as visit_start_date 
	, date_format(INTIME, 'hh:mm:ss') as visit_start_time 
	, OUTTIME as visit_end_date 
	, date_format(OUTTIME, 'hh:mm:ss') as visit_end_time 
	, 0 as visit_type_concept_id 
	, 0 as visit_source_concept_id 
from mimic3_icustays