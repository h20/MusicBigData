package recommendations.job.ratings;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import recommendations.job.input.InputCreator;

/*
 * hadoop jar /home/jeet/Documents/realtime/MusicRecommender/target/MusicRecommender-0.0.1-SNAPSHOT.jar recommendations.job.ratings.RatingDistributionJob /user/hduser/reco/input_sample /user/hduser/reco/rating_distribution
 */
public class RatingDistributionJob {
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Input Cleaner for Recommendation");
	    job.setJarByClass(InputCreator.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    job.setMapperClass(RatingDistributionMapper.class);
	    job.setReducerClass(RatingDistributionReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(LongWritable.class);
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
