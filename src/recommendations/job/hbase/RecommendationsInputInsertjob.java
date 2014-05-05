package recommendations.job.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
/*
 * Large time taken: 8mins, 27sec
 */
public class RecommendationsInputInsertjob {
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		//Configuration hadoopConf = new Configuration();
	    Configuration conf = HBaseConfiguration.create();
        conf.set("mapred.child.java.opts", "-Xmx1536m");
		Job job = new Job(conf, "Recommendations input HBase insertion job");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setJarByClass(RecommendationsInputInsertjob.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(RecommendationsInputInsertMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		TableMapReduceUtil.initTableReducerJob(args[1], RecommendationsInputInsertReducer.class, job);
		job.setNumReduceTasks(3);
		job.waitForCompletion(true);
	}

}
