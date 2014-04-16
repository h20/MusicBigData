package recommendations.job.hbase;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RecommendationInsertMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    String line = value.toString();
	    StringTokenizer tokenizer = new StringTokenizer(line);
	    Text userId = new Text(tokenizer.nextToken());
	    Text recommendations = new Text(tokenizer.nextToken());
        context.write(userId, recommendations);
    }
}
