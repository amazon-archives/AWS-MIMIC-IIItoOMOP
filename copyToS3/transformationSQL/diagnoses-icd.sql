select SUBJECT_ID as person_id
	, 0 as condition_concept_id 
	, null as condition_start_date 
	, null as condition_end_date 
	, 0 as condition_type_concept_id 
	, 'NA' as stop_reason
	, null as provider_id 
	, 0 as visit_occurrence_id 
	, ICD9_CODE as condition_source_value 
	, 0 as condition_source_concept_id  
from mimic3_diagnoses_icd