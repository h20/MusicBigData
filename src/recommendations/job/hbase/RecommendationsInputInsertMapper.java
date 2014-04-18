package recommendations.job.hbase;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RecommendationsInputInsertMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	/*private static Configuration conf = null;
	private static HTable table = null;*/
    /**
     * Initialization
     * @throws IOException 
     */
    /*void setConf() throws IOException {
        conf = HBaseConfiguration.create();
        table = new HTable(conf, "movie_recommendations");
    }
*/    
	Text userId = new Text();
	Text itemAndRating = new Text();
	byte[] familyBytes = Bytes.toBytes("input");
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		/*if (conf == null) {
			setConf();
		}
		Put put = new Put();
		table.put(put);*/
		
	    String line = value.toString();
	    StringTokenizer tokenizer = new StringTokenizer(line);
	    String user = tokenizer.nextToken();
	    String song = tokenizer.nextToken();
	    String rating = tokenizer.nextToken();
	    if (user != null && song != null && rating != null) {
	    	userId.set(user);
	    	itemAndRating.set(song+"::"+rating);
	    	context.write(userId, itemAndRating);
	    }
    }

}
