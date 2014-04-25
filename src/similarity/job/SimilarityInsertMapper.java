package similarity.job;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SimilarityInsertMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	Text song1Text = new Text();
	Text song2AndSimilarity = new Text();
	byte[] familyBytes = Bytes.toBytes("input");
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
	    StringTokenizer tokenizer = new StringTokenizer(line);
	    String song1 = tokenizer.nextToken();
	    String song2 = tokenizer.nextToken();
	    String similarity = tokenizer.nextToken();
	    if (song1 != null && song2 != null && similarity != null) {
	    	song1Text.set(song1);
	    	song2AndSimilarity.set(song2+"::"+similarity);
	    	context.write(song1Text, song2AndSimilarity);
	    }
    }

}
