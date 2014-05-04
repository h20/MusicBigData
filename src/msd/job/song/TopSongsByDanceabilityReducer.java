package msd.job.song;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counter;

public class TopSongsByDanceabilityReducer extends 
        Reducer<LongWritable, Text, Text, LongWritable> {
    
    protected void reduce(LongWritable key, Iterable<Text> values, 
            Context context) throws IOException, InterruptedException {
        Counter counter = null;
        long maxCount = 0;
        String topK = context.getConfiguration().get("topK");
        if (topK != null) {
            counter = context.getCounter("songs", "topK");
            maxCount = Long.parseLong(topK);
        }
        Iterator<Text> textIterator = values.iterator();
        while((counter == null || counter.getValue() < maxCount) 
                && textIterator.hasNext()) {
            context.write(textIterator.next(), key);
            if (counter != null) {
                counter.increment(1);
            }
        }
    }

}
