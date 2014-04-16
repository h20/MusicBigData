package recommendations.job.ratings;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RatingDistributionMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	
	private LongWritable ONE = new LongWritable(1);
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
	    StringTokenizer tokenizer = new StringTokenizer(line);
	    tokenizer.nextToken();
	    tokenizer.nextToken();
	    String rating = tokenizer.nextToken();
	    context.write(new Text(rating), ONE);
    }
}
