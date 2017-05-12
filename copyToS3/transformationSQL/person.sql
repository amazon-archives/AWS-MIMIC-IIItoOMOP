select SUBJECT_ID as person_id 
	, CASE when GENDER = 'M' then 8507 when GENDER = 'F' then 8532 else 8851 END as gender_concept_id 
	, year(DOB) as year_of_birth   
    , date_format(DOB, 'MM') as month_of_birth
    , date_format(DOB, 'dd') as day_of_birth
    , 0 as race_concept_id
    , 0 as ethnicity_concept_id
    , GENDER as gender_source_value
from mimic3_patients