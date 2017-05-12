drop table if exists death;

CREATE TABLE death
(person_id int,
death_date int,
death_concept_id int,
cause_concept_id int,
cause_source_value CHARACTER VARYING,
cause_source_concept_id int);

copy death
from 's3://${bucket}/stage/death.csv' 
credentials 'aws_iam_role=${redshift_arn}'
gzip
delimiter ','
ignoreheader 1;
