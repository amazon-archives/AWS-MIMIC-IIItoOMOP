select SUBJECT_ID as person_id
	, date_format(DOD, 'yyyyMMdd') as death_date 
	, 0 as death_type_concept_id 
	, 0 as cause_concept_id 
    , null as cause_source_value 
	, 0 as cause_source_concept_id 
from mimic3_patients 
where EXPIRE_FLAG = 1