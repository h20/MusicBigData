package msd.job.artist;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Counter;

public class TopArtistsReducer extends 
        Reducer<LongWritable, Text, Text, Text> {
    
    protected void reduce(LongWritable key, Iterable<Text> values, 
            Context context) throws IOException, InterruptedException {
        Counter counter = null;
        long maxCount = 0;
        String topK = context.getConfiguration().get("topK");
        if (topK != null) {
            counter = context.getCounter("artists", "topK");
            maxCount = Long.parseLong(topK);
        }
        Iterator<Text> textIterator = values.iterator();
        while((counter == null || counter.getValue() < maxCount) 
                && textIterator.hasNext()) {
            context.write(null, textIterator.next());
            if (counter != null) {
                counter.increment(1);
            }
        }
    }

}
