package msd.job.tags_count;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TagsAndArtistHotnessFilterReducer extends 
    Reducer<Text, LongWritable, Text, LongWritable> {
    
    LongWritable averageHotness = new LongWritable();
    
    protected void reduce(Text key, Iterable<LongWritable> values, 
            Context context) throws IOException, InterruptedException {
        long count = 0;
        Iterator<LongWritable> valueIterator = values.iterator();
        while (valueIterator.hasNext()) {
            valueIterator.next();
            count++;
        }
        averageHotness.set(count);
        context.write(key, averageHotness);
    }

}
