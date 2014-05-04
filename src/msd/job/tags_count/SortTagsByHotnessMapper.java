package msd.job.tags_count;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortTagsByHotnessMapper extends 
    Mapper<LongWritable, Text, LongWritable, Text> {
    
    LongWritable hotnessWritable = new LongWritable();
    Text tagText = new Text();
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        String tag = split[0];
        long hotness = Long.parseLong(split[1]);
        tagText.set(tag);
        hotnessWritable.set(hotness);
        context.write(hotnessWritable, tagText);
    }
}
