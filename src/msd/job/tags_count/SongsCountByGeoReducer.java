package msd.job.tags_count;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SongsCountByGeoReducer 
    extends Reducer<Text, Text, Text, Text> {
    
    Text countLong = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> value,
            Context context) throws IOException, InterruptedException {
        Iterator<Text> valueIterator = value.iterator();
        long count = 0;
        String location = "";
        while(valueIterator.hasNext()) {
            if (count == 0) {
                location = valueIterator.next().toString();
            } else {
                valueIterator.next();
            }
            count++;
        }
        countLong.set(location + "\t" + count);
        context.write(key, countLong);
    }
}
