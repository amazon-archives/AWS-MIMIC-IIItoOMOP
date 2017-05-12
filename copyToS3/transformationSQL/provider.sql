select CGID as provider_id 
	, CGID as provider_source_value 
	, 0 as specialty_concept_id 
	, LABEL as specialty_source_value 
	, 0 as specialty_source_concept_id 
from mimic3_caregivers