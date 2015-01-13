import java.io.IOException;
 
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
 
 
public class topFivePerGenreMapper extends Mapper <LongWritable, Text, Text, Text> {
 
    private String genre, movieTitle, rating;
     
    @Override
    public void map(LongWritable key, Text values, Context output) throws IOException, InterruptedException {
         
        // Get one record and split the data
        String[] record = values.toString().split(""); // - Weird Hive delimiter
         
        // Get the right data
        genre = record[0].trim();
        movieTitle = record[1].trim();
        rating = record[2].trim();
         
        // Send it out to the reducer
        output.write(new Text(genre), new Text((movieTitle + "~" + rating)));
         
    }
     
}