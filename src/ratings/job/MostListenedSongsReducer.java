package ratings.job;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MostListenedSongsReducer 
    extends Reducer<Text, LongWritable, Text, LongWritable> {
    
    LongWritable countLongWritable = new LongWritable();
    
    @Override
    public void reduce(Text songKey, Iterable<LongWritable> countIterable, 
            Context context)  throws IOException, InterruptedException {
        long count = 0;
        Iterator<LongWritable> countIterator = countIterable.iterator();
        while(countIterator.hasNext()) {
            count = count + countIterator.next().get();
        }
        countLongWritable.set(count);
        context.write(songKey, countLongWritable);
    }

}
