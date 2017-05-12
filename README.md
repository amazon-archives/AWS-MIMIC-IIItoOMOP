# Build a Healthcare Data Warehouse using Spark and the OHDSI Common Data Model

This code demonstrates the architecture featured on the AWS big data blog (https://aws.amazon.com/blogs/big-data/) on creating a healthcare data warehouse using Redshift, Spark on EMR and Lambda that was published on May 12, 2017.  It takes 
an openly available research dataset called MIMIC-III and converts it into a standard open source healthcare data model called OMOP.


## Prerequisites

* You must have an AWS account
* You must have access to the MIMIC-III dataset
	* You can request access here: https://s3.amazonaws.com/physionet-pds/PhysioBank/mimic3cdb/index.html 
* You must have a S3 endpoint configured


## Steps

1. Create a bucket in S3
2. Within the bucket created in step 1, copy over all of the directories underneath the "copyToS3" directory located in this GitHub repo.
	* Make sure you do not copy the actual "copyToS3" directory itself
3. Upload the MIMIC-III raw data in csv format within the subdirectories of the mimic3 folder that you copied to s3 in step 2
	* eg - caregivers.csv would be "mimic3/caregivers/caregivers.csv"
3. Copy the "cloudformation", “redshiftSQL”,  “transformationSQL”, and “config” folders from GitHub into the bucket created in step 1
4. Open the config.json file in the "config" directory that was just copied and replace all references of "<your s3 bucket>" to the name of the bucket you created in step 1
	* Also, ensure that your source file names from the mimic3 data match what is listed in this file.  If they do not match, either rename the files or change the references in the config file.
5. Create a folder called “jars” and upload both the "RedshiftCopier-1.0.0-jar-with-dependencies.jar" and "SparkBatchProcessor-1.0.0-jar-with-dependencies.jar" jars within that folder 
	* These jars will be produced after you build the project.
6. Create a new Stack in CloudFormation called "Redshift" and reference the redshift.json template in the "cloudformation" folder within S3.
	* You will need to pass the following parameters to this template:
		1. "Security Group" - this should be the default security group in your VPC
		2. "Subnet Id" - the public subnet of your default VPC
		3. "IP" - the IP range that you would like to be able to connect to the Redshift instance
		4. "Username" - the Redshift Username
		5. "Password" - the Redshift password
7. Once step 6 has completed successfully, create a new Stack in CloudFormation and reference the mimic3-ohdsi.json template in the "cloudformation" folder within S3.
    * You will need to pass the following parameters to this template:
		1. "Security Group" - this should be the default security group in your VPC
		2. "Subnet Id" - the public subnet of your default VPC
		3. "RedshiftRoleArn" - this is the ARN for the IAM role created in the Redshift CloudFormation template 
		4. "Username" - the Redshift Username
		5. "Password" - the Redshift password
		6. "RedshiftEndpoint" - the endpoint of the Redshift cluster created in step 6
		7. "Bucket" - the bucket created in step 1
8. Sit back and enjoy 
	* The CloudFormation template in step 8 creates an EMR cluster with a Spark job that executes automatically and tears down the infrastructure created in step 7 once all of the data has been loaded into Redshift


## Notes

* You will incur a small amount of charges for running this code on AWS.
* You will need to tear down the CloudFormation stack created in step 6 manually.
* An EC2 instance is spun up in the Redshift CloudFormation stack for the purpose of adding an IAM role to the cluster since CloudFormation does not support this directly.