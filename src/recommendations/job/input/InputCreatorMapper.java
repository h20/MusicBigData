package recommendations.job.input;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InputCreatorMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    String line = value.toString();
	    StringTokenizer tokenizer = new StringTokenizer(line);
	    StringBuilder lineBuilder = new StringBuilder();
	    String userId = tokenizer.nextToken();
	    String item = tokenizer.nextToken();
	    String rating = tokenizer.nextToken();
	    lineBuilder.append(userId);
	    lineBuilder.append(",");
	    lineBuilder.append(item);
	    lineBuilder.append(",");
	    lineBuilder.append(rating);
	    context.write(new Text(lineBuilder.toString()), new Text());
    }
}
