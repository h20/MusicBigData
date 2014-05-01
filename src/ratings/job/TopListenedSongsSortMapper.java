package ratings.job;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopListenedSongsSortMapper 
    extends Mapper<LongWritable, Text, LongWritable, Text> {
    
    LongWritable ratingLongWritable = new LongWritable();
    Text songText = new Text();
    
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        String songID = tokenizer.nextToken();
        long rating = Long.parseLong(tokenizer.nextToken());
        songText.set(songID);
        ratingLongWritable.set(rating);
        context.write(ratingLongWritable, songText);
    }
}
