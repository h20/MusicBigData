package ratings.job;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MostListenedSongsMapper extends 
    Mapper<LongWritable, Text, Text, LongWritable> {
	
    private Text songIdText = new Text();
    private LongWritable countLongWritable = new LongWritable();
	
    @Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
	    String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        tokenizer.nextToken();
        String songId = tokenizer.nextToken();
        String count = tokenizer.nextToken();
        songIdText.set(songId);
        countLongWritable.set(Long.parseLong(count));
        context.write(songIdText, countLongWritable);
	}

}
