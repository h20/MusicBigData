package recommendations.job.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

public class RecommendationInsertReducer extends TableReducer<Text, Text, Text>  {
	
	private byte[] rawUpdateColumnFamily = Bytes.toBytes("item_based");
	
	@Override
	public void reduce(Text keyin, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	// aggregate counts
	for (Text val : values) {
	   // put date in table
	   String value = val.toString();
	   value = value.substring(1, value.length() - 1);
	   String [] itemRecos = value.split(",");
	   for (String itemReco : itemRecos) {
		   String [] itemRecoSplit = itemReco.split(":");
		   String item = itemRecoSplit[0];
		   String rating = itemRecoSplit[1];
		   Put put = new Put(keyin.toString().getBytes());
		   put.add(rawUpdateColumnFamily, Bytes.toBytes(item), Bytes.toBytes(rating));
		   context.write(keyin, put);
	   }
	}
  }
}
