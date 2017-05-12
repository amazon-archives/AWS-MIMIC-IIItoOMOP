select SUBJECT_ID as person_id 
	, date_format(CHARTDATE, 'yyyyMMdd') as procedure_date 
	, CPT_CD as procedure_source_value 
from mimic3_cptevents