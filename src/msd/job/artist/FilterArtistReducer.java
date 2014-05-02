package msd.job.artist;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FilterArtistReducer extends 
        Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        Text value = values.iterator().next();
        if (value != null) {
            context.write(null, value);
        }
    }
    
}
