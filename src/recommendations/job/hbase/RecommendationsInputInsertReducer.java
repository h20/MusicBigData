package recommendations.job.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

public class RecommendationsInputInsertReducer extends TableReducer<Text, Text, Text> {
	
	
	byte[] familyBytes = Bytes.toBytes("input");
	@Override
	public void reduce(Text keyin, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Put put = new Put(Bytes.toBytes(keyin.toString()));
		for (Text value : values) {
			String itemRating[] = value.toString().split("::");
			put.add(familyBytes, Bytes.toBytes(itemRating[0]), Bytes.toBytes(itemRating[1]));
		}
		if (keyin.toString().equals("fd50c4007b68a3737fe052d5a4f78ce8aa117f3d")) {
			String userId = "fd50c4007b68a3737fe052d5a4f78ce8aa117f3d";
			put.add(familyBytes, Bytes.toBytes("SOAAADD12AB018A9DD"), Bytes.toBytes("0"));
			context.write(new Text(userId), put);
		} else {
			context.write(keyin, put);
		}
	}
}
