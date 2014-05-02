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
        Counter counter = context.getCounter("artists", "top500");
        Iterator<Text> textIterator = values.iterator();
        while(textIterator.hasNext() && counter.getValue() < 500) {
            counter.increment(1);
            context.write(null, textIterator.next());
        }
    }

}
