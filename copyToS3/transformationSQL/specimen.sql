select le.SUBJECT_ID as person_id 
	, 0 as specimen_concept_id 
	, 0 as specimen_type_concept_id 
	, le.CHARTTIME as specimen_date 
	, date_format(le.CHARTTIME, 'hh:mm:ss') as specimen_time 
	, 0 as unit_concept_id 
	, 0 as anatomic_site_concept_id 
	, 0 as disease_status_concept_id 
	, d.LOINC_CODE as specimen_source_id 
    , d.FLUID as specimen_source_value 
	, le.VALUEUOM as unit_source_value 
from mimic3_d_labitems d inner join mimic3_labevents le on d.ITEMID =  le.ITEMID