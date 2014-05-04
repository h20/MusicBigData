package msd.job.tags_count;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortTagsByHotnessReducer extends 
    Reducer<LongWritable, Text, Text, LongWritable> {
    
    protected void reduce(LongWritable key, Iterable<Text> values,
        Context context) throws IOException, InterruptedException {
        Iterator<Text> valueIterator = values.iterator();
        while (valueIterator.hasNext()) {
            context.write(valueIterator.next(), key);
        }
    }

}
