package recommendations.job.input;


import java.io.BufferedInputStream;

import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * hadoop jar MusicBigData-0.0.1-SNAPSHOT.jar recommendations.job.input.InputCreator /user/hduser/music_data_small /user/hduser/music_data_csv_small
 */
public class InputCreator {
	
	
	public static void main(String args[]) throws Exception, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		//System.out.println("before: " + conf.get("mapred.child.java.opts"));
		conf.set("mapred.child.java.opts", "-Xmx1024m");
		//System.out.println("after: " + conf.get("mapred.child.java.opts"));
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
