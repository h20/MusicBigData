package ratings.job;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

public class TopListenedSongsSortReducer extends 
    Reducer<LongWritable, Text, Text, LongWritable> {
    
    public void reduce(LongWritable key, Iterable<Text> value, Context context)
            throws IOException, InterruptedException {
        Iterator<Text> songsIterator = value.iterator();
        Counter counter = null;
        long maxCount = 0;
        String topK = context.getConfiguration().get("topK");
        if (topK != null) {
            counter = context.getCounter("count", "topK");
            maxCount = Long.parseLong(topK);
        }
        
        while((counter == null || counter.getValue() < maxCount) 
                && songsIterator.hasNext()) {
            context.write(songsIterator.next(), key);
            if (counter != null) {
                counter.increment(1);
            }
        }
    }
}
