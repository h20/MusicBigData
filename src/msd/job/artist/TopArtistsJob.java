package msd.job.artist;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TopArtistsJob extends Configured implements Tool {
    
    public static void main(String args[]) throws 
            Exception {
        long startTime = System.currentTimeMillis();
        ToolRunner.run(
                new Configuration(), new TopArtistsJob(), args);
        long endTime = System.currentTimeMillis();
        System.out.println("Time: " + (endTime - startTime)/1000.0);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set("mapred.textoutputformat.separator", "");
        Job job = new Job(getConf(), "Top artists job");
        job.setJarByClass(TopArtistsJob.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(TopArtistsMapper.class);
        job.setReducerClass(TopArtistsReducer.class);
        job.setSortComparatorClass(LongWritable.DecreasingComparator.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

}
