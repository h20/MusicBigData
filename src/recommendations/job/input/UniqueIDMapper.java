package recommendations.job.input;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UniqueIDMapper extends 
        Mapper<LongWritable, Text, Text, Text> {
    
    Text filterText = new Text();
    Text emptyText = new Text();
    
    @Override
    protected void map(LongWritable key, Text value,Context context)
            throws IOException, InterruptedException {
        int index = context.getConfiguration().getInt("index", 0);
        String filter = value.toString().split("\t")[index];
        filterText.set(filter);
        context.write(filterText, emptyText);
    }

}
