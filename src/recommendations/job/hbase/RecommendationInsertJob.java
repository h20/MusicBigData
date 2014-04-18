package recommendations.job.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/*
 * hadoop jar /home/jeet/Documents/RTBDA/reco/reco.jar recommendations.job.hbase.RecommendationInsertJob output_sample.txt
 */
public class RecommendationInsertJob {
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Recommendations: HBase insertion job");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setJarByClass(RecommendationInsertJob.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(RecommendationInsertMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		TableMapReduceUtil.initTableReducerJob(args[1], RecommendationInsertReducer.class, job);
		job.waitForCompletion(true);
		/*Configuration conf = new Configuration();
		Job job = new Job(conf, "Recommendations: HBase insertion job");
		conf.set("mapred.child.java.opts", "-Xmx1024m");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setJarByClass(RecommendationInsertJob.class);
		job.setInputFormatClass(TextInputFormat.class);
		//job.setMapperClass(RecommendationInsertMapper.class);
		job.setMapperClass(RecommendationsInsertHbaseMapper.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);  
		job.setMapOutputValueClass(Put.class);
		job.setSpeculativeExecution(false);  
        job.setReduceSpeculativeExecution(false);  
        job.setInputFormatClass(TextInputFormat.class);  
        job.setOutputFormatClass(HFileOutputFormat.class);
        FileOutputFormat.setOutputPath(job,new Path("/user/hduser/music_hbase"));
        HTable hTable = new HTable(conf, "movie_recommendations");
        HFileOutputFormat.configureIncrementalLoad(job, hTable);
        
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		TableMapReduceUtil.initTableReducerJob("movie_recommendations", RecommendationInsertReducer.class, job);
		job.waitForCompletion(true);*/
	}
}
