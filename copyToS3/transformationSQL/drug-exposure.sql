select cv.SUBJECT_ID as person_id 
	, 0 as drug_concept_id 
	, date_format(cv.CHARTTIME, 'yyyyMMdd') as drug_exposure_start_date 
	, 0 as drug_type_concept_id 
	, cv.AMOUNT as effective_drug_dose 
	, cv.CGID as provider_id 
	, cv.ICUSTAY_ID as visit_occurrence_id 
	, regexp_replace(items.LABEL, ',', ';') as drug_source_value 
	, 0 as drug_source_concept_id 
	, cv.ORIGINALROUTE as route 
	, cv.AMOUNTUOM as doseuom 
from mimic3_inputevents_cv cv 
	inner join mimic3_d_items items on cv.ITEMID = items.ITEMID 

union all 

select mv.SUBJECT_ID as person_id 
	, 0 as drug_concept_id 
	, date_format(mv.STARTTIME, 'yyyyMMdd') as drug_exposure_start_date 
	, 0 as drug_type_concept_id 
	, mv.AMOUNT as effective_drug_dose 
	, mv.STORETIME as provider_id 
	, mv.ICUSTAY_ID as visit_occurrence_id 
	, regexp_replace(items.LABEL, ',', ';') as drug_source_value 
	, 0 as drug_source_concept_id 
	, null	as route 
	, mv.AMOUNTUOM as doseuom 
from mimic3_inputevents_mv mv
	inner join mimic3_d_items items on mv.ITEMID = items.ITEMID 