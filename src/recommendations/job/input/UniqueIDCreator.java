package recommendations.job.input;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UniqueIDCreator {
    
    /*
     * For users the task took 3 mins
     * For items the task took 3 mins
     */
    public static void main(String args[]) 
            throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", "");
        conf.setInt("index", Integer.parseInt(args[2]));
        Job job = new Job(conf, "Unique ID job");
        job.setJarByClass(UniqueIDCreator.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(UniqueIDMapper.class);
        job.setReducerClass(UniqueUIReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
