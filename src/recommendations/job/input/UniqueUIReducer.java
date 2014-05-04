package recommendations.job.input;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UniqueUIReducer extends 
        Reducer<Text, Text, Text, Text> {
    Text text = new Text();
    protected void reduce(Text arg0, Iterable<Text> arg1,
            Context arg2)
            throws IOException, InterruptedException {
        arg2.write(arg0, text);
    }

}
