package msd.job.tags;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SongsCountByYearReducer 
    extends Reducer<Text, LongWritable, Text, LongWritable> {
    
    LongWritable countLong = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> value,
            Context context) throws IOException, InterruptedException {
        Iterator<LongWritable> valueIterator = value.iterator();
        long count = 0;
        while(valueIterator.hasNext()) {
            count = count + valueIterator.next().get();
        }
        countLong.set(count);
        context.write(key, countLong);
    }
}
