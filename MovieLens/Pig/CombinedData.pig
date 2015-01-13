-- Register the age grouper, an UDF for defining the age group of a record
REGISTER 'hdfs://localhost:9000/MovieLens/udf/ageGrouper.jar';

-- Output age groups are:
--	18 --> <18
--	24 --> 18-24
--	34 --> 25-34
--	44 --> 35-44
--	49 --> 45-49
-- 	56 --> 50-56
--	57 --> 56+


-- Load the rating records
ratingsData = 	LOAD '/MovieLens/Data/records/u.data'
				AS (userId:int, movieId:int, rating:int, timeStamp:int);

-- Load the user data records
userData =	LOAD '/MovieLens/Data/records/u.user'
			USING PigStorage('|')
			AS (userId:int, age:int, gender:chararray, occupation:chararray, zipCode:int);
			
-- Load the movie data records
movieData = LOAD '/MovieLens/Data/records/u.item'
			USING PigStorage('|')
			AS (
				movieId:int,
				title:chararray,
				releaseDate:chararray,
				videoReleaseData:chararray,
				imdbUrl:chararray,
				unknown:int,
				action:int,
				adventure:int,
				animation:int,
				children:int,
				comedy:int,
				crime:int,
				documentary:int,
				drama:int,
				fantasy:int,
				filmNoir:int,
				horror:int,
				musical:int,
				mystery:int,
				romance:int,
				scifi:int,
				thriller:int,
				war:int,
				western:int
			);

			
-- First combine the ratings data with the user data
userAndRatingsData = JOIN ratingsData BY userId, userData BY userId;

-- Then combine the previously combined data with the movie data, to create a complete record with all data
completeData = JOIN userAndRatingsData BY movieId, movieData BY movieId;


-- Only keep the necessary fields
data = FOREACH completeData GENERATE
			age AS age,
			title AS title,
			rating AS rating,
			unknown AS unknown,
			action AS action,
			adventure AS adventure,
			animation AS animation,
			children AS children,
			comedy AS comedy,
			crime AS crime,
			documentary AS documentary,
			drama AS drama,
			fantasy AS fantasy,
			filmNoir AS filmNoir,
			horror AS horror,
			musical AS musical,
			mystery AS mystery,
			romance AS romance,
			scifi AS scifi,
			thriller AS thriller,
			war AS war,
			western AS western;
			
			
-- Split the data according to the genre
SPLIT data INTO
	unknown IF unknown == 1,
	action IF action == 1,
	adventure IF adventure == 1,
	animation IF animation == 1,
	children IF children == 1,
	comedy IF comedy == 1,
	crime IF crime == 1,
	documentary IF documentary == 1,
	drama IF drama == 1,
	fantasy IF fantasy == 1,
	filmNoir IF filmNoir == 1,
	horror IF horror == 1,
	musical IF musical == 1,
	mystery IF mystery == 1,
	romance IF romance == 1,
	scifi IF scifi == 1,
	thriller IF thriller == 1,
	war IF war == 1,
	western IF western == 1;


-- Determine an age group through the UDF, name the genre and only take the necessary fields
unknown_records =	 	FOREACH unknown GENERATE ageGrouper(age) AS age, 'unknown' AS genre, title AS title, rating AS rating;
action_records = 		FOREACH action GENERATE ageGrouper(age) AS age, 'action' AS genre, title AS title, rating AS rating;
adventure_records = 	FOREACH adventure GENERATE ageGrouper(age) AS age, 'adventure' AS genre, title AS title, rating AS rating;
animation_records = 	FOREACH animation GENERATE ageGrouper(age) AS age,'animation' AS genre, title AS title, rating AS rating;
children_records = 		FOREACH children GENERATE ageGrouper(age) AS age, 'children' AS genre, title AS title, rating AS rating;
comedy_records = 		FOREACH comedy GENERATE ageGrouper(age) AS age, 'comedy' AS genre, title AS title, rating AS rating;
crime_records = 		FOREACH crime GENERATE ageGrouper(age) AS age, 'crime' AS genre, title AS title, rating AS rating;
documentary_records = 	FOREACH documentary GENERATE ageGrouper(age) AS age, 'documentary' AS genre, title AS title, rating AS rating;
drama_records = 		FOREACH drama GENERATE ageGrouper(age) AS age, 'drama' AS genre, title AS title, rating AS rating;
fantasy_records = 		FOREACH fantasy GENERATE ageGrouper(age) AS age, 'fantasy' AS genre, title AS title, rating AS rating;
filmNoir_records = 		FOREACH filmNoir GENERATE ageGrouper(age) AS age, 'filmNoir' AS genre, title AS title, rating AS rating;
horror_records = 		FOREACH horror GENERATE ageGrouper(age) AS age, 'horror' AS genre, title AS title, rating AS rating;
musical_records =	 	FOREACH musical GENERATE ageGrouper(age) AS age, 'musical' AS genre, title AS title, rating AS rating;
mystery_records = 		FOREACH mystery GENERATE ageGrouper(age) AS age, 'mystery' AS genre, title AS title, rating AS rating;
romance_records = 		FOREACH romance GENERATE ageGrouper(age) AS age, 'romance' AS genre, title AS title, rating AS rating;
scifi_records = 		FOREACH scifi GENERATE ageGrouper(age) AS age, 'scifi' AS genre, title AS title, rating AS rating;
thriller_records =	 	FOREACH thriller GENERATE ageGrouper(age) AS age, 'thriller' AS genre, title AS title, rating AS rating;
war_records = 			FOREACH war GENERATE ageGrouper(age) AS age, 'war' AS genre, title AS title, rating AS rating;
western_records = 		FOREACH western GENERATE ageGrouper(age) AS age, 'western' AS genre, title AS title, rating AS rating;


-- Union everything back to one variable for writing it back to the HDFS
result = UNION 	unknown_records, action_records, adventure_records, animation_records,
				children_records, comedy_records, crime_records, documentary_records,
				drama_records, fantasy_records, filmNoir_records, horror_records,
				musical_records, mystery_records, romance_records, scifi_records,
				thriller_records, war_records, western_records;

-- Store the results into the HDFS
STORE result INTO '/MovieLens/Data/output/pig';