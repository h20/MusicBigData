package ratings.job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MostListenedSongsJob {

	public static void main(String[] args) throws 
	        IOException, ClassNotFoundException, InterruptedException {
	    Configuration conf = new Configuration();
	    Job job = new Job(conf, "Find most listened songs");
	    job.setJarByClass(MostListenedSongsJob.class);
	    job.setMapperClass(MostListenedSongsMapper.class);
	    job.setReducerClass(MostListenedSongsReducer.class);
	    job.setCombinerClass(MostListenedSongsReducer.class);
	    job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(LongWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    job.waitForCompletion(true);
	}
}
