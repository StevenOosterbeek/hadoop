-- Register mongo jar dependencies
REGISTER '/usr/local/hadoop/lib/mongo-java-driver-2.12.3.jar'  
REGISTER '/usr/local/hadoop/lib/mongo-hadoop-core-1.3.2-SNAPSHOT.jar'
REGISTER '/usr/local/hadoop/lib/mongo-hadoop-pig-1.3.2-SNAPSHOT.jar'


-- Load the data out of the MongoDB
data = 	LOAD 'mongodb://localhost/cityOfChicago.employees'
		USING com.mongodb.hadoop.pig.MongoLoader('annual_salary:int, name:chararray, department:chararray, job_title:chararray')
		AS (salary, name, department, job);


-- Group by department and job
groupedData = GROUP data BY (department, job);

-- Calculate the average annual salary
result = FOREACH groupedData GENERATE
			FLATTEN(group) AS (department, job),
			AVG(data.salary) AS avg_salary;


DUMP result;