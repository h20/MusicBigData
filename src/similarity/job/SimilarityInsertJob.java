package similarity.job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class SimilarityInsertJob {
    /*
     * Time taken: 4mins, 50sec
     */
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		//Configuration hadoopConf = new Configuration();
	    Configuration conf = HBaseConfiguration.create();
        conf.set("mapred.child.java.opts", "-Xmx1536m");
		Job job = new Job(conf, "Similarity output HBase insertion job");
		job.getConfiguration().set("familyBytes", args[2]);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setJarByClass(SimilarityInsertJob.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(SimilarityInsertMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		TableMapReduceUtil.initTableReducerJob(args[1], SimilarityInsertReducer.class, job);
		job.setNumReduceTasks(2);
		job.waitForCompletion(true);
	}
}
