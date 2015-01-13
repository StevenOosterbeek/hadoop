import java.io.IOException;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 
public class topFivePerGenre {
 
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
         
        // Check if the input/output paths are set
        if (args.length != 2) {
            System.err.println("You need to set an input and output path! (topFivePerGenre <input> <output>)");
            System.exit(-1);
        }
         
        // Create the job
        Job job = Job.getInstance(new Configuration(), "Top five movies per genre");
        job.setJarByClass(topFivePerGenre.class);
         
        // Set the output data types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
         
        // Set the mapper and reducer
        job.setMapperClass(topFivePerGenreMapper.class);
        job.setReducerClass(topFivePerGenreReducer.class);
         
        // Set the input/output paths
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
         
        // Exit when the job is completed
        System.exit(job.waitForCompletion(true) ? 0 : 1);
         
    }
 
}