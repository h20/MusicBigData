package ratings.job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopListenedSongsSortJob {
    
    public static void main(String args[]) throws 
            IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Find top listened songs");
        job.setJarByClass(TopListenedSongsSortJob.class);
        job.setMapperClass(TopListenedSongsSortMapper.class);
        job.setReducerClass(TopListenedSongsSortReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        if (args.length == 3 && args[2] != null) {
            job.getConfiguration().set("topK", args[2]);
        }
        job.setSortComparatorClass(LongWritable.DecreasingComparator.class);
        job.waitForCompletion(true);
    }

}
