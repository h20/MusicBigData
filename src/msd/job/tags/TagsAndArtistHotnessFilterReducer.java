package msd.job.tags;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TagsAndArtistHotnessFilterReducer extends 
    Reducer<Text, DoubleWritable, Text, Text> {
    
    Text averageHotness = new Text();
    
    protected void reduce(Text key, Iterable<DoubleWritable> values, 
            Context context) throws IOException, InterruptedException {
        long count = 0;
        double runningMean = 0.0d;
        Iterator<DoubleWritable> valueIterator = values.iterator();
        while (valueIterator.hasNext()) {
            /*runningMean = (runningMean * (count - 1))/count 
                    + valueIterator.next().get()/count;*/
            runningMean += valueIterator.next().get();
            count++;
        }
        if (count != 0) {
            runningMean = runningMean/count;
        }
        averageHotness.set((long)(runningMean * 10000)+"");
        context.write(key, averageHotness);
    }

}
