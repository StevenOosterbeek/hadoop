-- Load all Pig output records
CREATE EXTERNAL TABLE IF NOT EXISTS allData (

	ageGroup INT,
	genre STRING,
	movieTitle STRING,
	rating INT

) ROW FORMAT DELIMITED

	FIELDS TERMINATED BY '\t'
	LINES TERMINATED BY '\n'

LOCATION '/MovieLens/Data/output/pig/';


-- Create files per age group
INSERT OVERWRITE DIRECTORY '/MovieLens/Data/output/hive/tillEighteen'
SELECT genre, movieTitle, rating FROM allData WHERE ageGroup == '18';

INSERT OVERWRITE DIRECTORY '/MovieLens/Data/output/hive/eighteenTillTwentyfour'
SELECT genre, movieTitle, rating FROM allData WHERE ageGroup == '24';

INSERT OVERWRITE DIRECTORY '/MovieLens/Data/output/hive/twentyfiveTillThirtyfour'
SELECT genre, movieTitle, rating FROM allData WHERE ageGroup == '34';

INSERT OVERWRITE DIRECTORY '/MovieLens/Data/output/hive/thirtyfiveTillFortyfour'
SELECT genre, movieTitle, rating FROM allData WHERE ageGroup == '44';

INSERT OVERWRITE DIRECTORY '/MovieLens/Data/output/hive/fortyfourTillFortynine'
SELECT genre, movieTitle, rating FROM allData WHERE ageGroup == '49';

INSERT OVERWRITE DIRECTORY '/MovieLens/Data/output/hive/fortynineTillFiftysix'
SELECT genre, movieTitle, rating FROM allData WHERE ageGroup == '56';

INSERT OVERWRITE DIRECTORY '/MovieLens/Data/output/hive/fiftysixPlus'
SELECT genre, movieTitle, rating FROM allData WHERE ageGroup == '57';
