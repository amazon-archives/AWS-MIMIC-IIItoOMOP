select le.SUBJECT_ID as person_id 
	, 0 as measurement_concept_id 
    , date_format(le.CHARTTIME, 'yyyyMMdd') as measurement_date 
    , date_format(le.CHARTTIME, 'hh:mm:ss') as measurement_time 
    , 0 measurement_type_concept_id 
    , le.VALUENUM as value_as_number 
    , icu.ICUSTAY_ID as visit_occurrence_id 
    , li.LOINC_CODE as measurement_source_value 
    , le.VALUEUOM as unit_source_value 
from mimic3_d_labitems li 
	inner join mimic3_labevents le on li.ITEMID = le.ITEMID 
    left join mimic3_icustays icu on le.HADM_ID = icu.HADM_ID