package recommendations.job.input;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * hadoop jar /home/jeet/Documents/realtime/MusicRecommender/target/MusicRecommender-0.0.1-SNAPSHOT.jar recommendations.job.input.InputCreator /user/hduser/reco/input_sample /user/hduser/reco/input_clean
 */
public class InputCreator {
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Input Cleaner for Recommendation");
	    job.setJarByClass(InputCreator.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    job.setMapperClass(InputCreatorMapper.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
