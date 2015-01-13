import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
 
public class topFivePerGenreReducer extends Reducer <Text, Text, Text, Text> {
	
	// Map for buffering the movies and saving the movies with their final rating
	private MapWritable movieBuffer = new MapWritable();
	private MapWritable finalList = new MapWritable();
 
    public void reduce (Text genre, Iterable<Text> titleAndRatings, Context output) throws IOException, InterruptedException {
    	
    	// Fill up the movie buffer
    	for (Text data : titleAndRatings) {

    		// Get the right data
    		String dataString = data.toString();
    		String[] dataArray = dataString.split("~");
    		
    		Text movieTitle = new Text(dataArray[0]);
    		String rating = dataArray[1];
    		
    		// Use one string for saving the total rating and total number of ratings within the map
    		Text ratingData = null;
    		
    		if (movieBuffer.get(movieTitle) == null) {
    			
    			// Add this movie to the buffer with his first rating
    			ratingData = new Text(rating + "/1");
    			
    			
    		} else {
    			
    			// Update the rating if the movie is already in the buffer
    			String[] currentRatingData = movieBuffer.get(movieTitle).toString().split("/");
    			
    			Integer currentRating = Integer.parseInt(currentRatingData[0]);
    			Integer currentNumberOfRatings = Integer.parseInt(currentRatingData[1]);
    			
    			String newRating = Integer.toString(currentRating + Integer.parseInt(rating));
    			String newNumberOfRatings = Integer.toString(currentNumberOfRatings + 1);
    			
    			ratingData = new Text(newRating + "/" + newNumberOfRatings);
    			
    		}
    		
    		movieBuffer.put(movieTitle, ratingData);
    		
    	}
    	
    	
    	// Generate a list with the final ratings per movie
    	for (Map.Entry<Writable, Writable> movieRecord : movieBuffer.entrySet()) {
    		
    		String[] ratingsData = movieRecord.getValue().toString().split("/");
    		Integer finalRating = Integer.parseInt(ratingsData[0]) / Integer.parseInt(ratingsData[1]);
    		
    		finalList.put(movieRecord.getKey(), new IntWritable(finalRating));
    		
    	}

    	    	
    	// Generate the final top 5 array
    	Integer[] topFiveRatings = new Integer[5];
    	String[] topFiveTitles = new String[5];
    	
    	for (Map.Entry<Writable, Writable> movie : finalList.entrySet()) {
    		
    		Integer rating = Integer.parseInt(movie.getValue().toString());
    		
    		for (int i = 0; i < 5; i++) {
    			
    			if (topFiveRatings[i] == null) {
    				
    				topFiveRatings[i] = rating;
    				topFiveTitles[i] = movie.getKey().toString();
    				
    			} else if (topFiveRatings[i] < rating) {
    				
    				topFiveRatings[i] = rating;
    				topFiveTitles[i] = movie.getKey().toString();
    				break;
 
    			}
    			
    		}
    	
    	}
    	
    	
    	// Generate the final top 5 string
    	String topFive = new String();
    	
    	for (int j = 0; j < 5; j++) {

    		topFive += "(" + (j + 1) + ") " + topFiveTitles[j];
    		if (j != 4) topFive += " // ";
    		
    	}
    	
    	output.write(genre, new Text(topFive));
         
    }
     
}