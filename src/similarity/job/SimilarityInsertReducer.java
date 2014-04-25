package similarity.job;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

public class SimilarityInsertReducer extends TableReducer<Text, Text, Text> {
	
	
	static byte[] familyBytes = null;
	/*@Override
	public void reduce(Text keyin, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		if (familyBytes == null) {
			familyBytes = Bytes.toBytes(context.getConfiguration().get("familyBytes"));
		}
		Put put = new Put(Bytes.toBytes(keyin.toString()));
		for (Text value : values) {
			String itemRating[] = value.toString().split("::");
			put.add(familyBytes, Bytes.toBytes(itemRating[0]), Bytes.toBytes(itemRating[1]));
		}
		context.write(keyin, put);
	}*/
	
	static Map<String, String> songsIdMap;
	
	public static Map<String, String> getIdMapFromFileSong(FileSystem fileSystem, Path path) throws IOException {
		if (fileSystem.exists(path)) {
			FSDataInputStream in = fileSystem.open(path);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			Map<String, String> map = new HashMap<String, String>();
			String line;
			while((line = br.readLine()) != null) {
				String[] values = line.split(" ", -1);
				map.put(values[1], values[0]);
			}
			br.close();
			in.close();
			return map;
		}
		return null;
	}
	
	@Override
	public void reduce(Text keyin, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	// aggregate counts
		FileSystem fileSystem = FileSystem.get(context.getConfiguration());
		Path songsPath = new Path("/user/hduser/music_songs_small");
		
		if (songsIdMap == null) {
			songsIdMap = getIdMapFromFileSong(fileSystem, songsPath);
		}
		if (familyBytes == null) {
			familyBytes = Bytes.toBytes(context.getConfiguration().get("familyBytes"));
		}
		Put put = new Put(Bytes.toBytes(songsIdMap.get(keyin.toString())));
		for (Text value : values) {
			String itemSimilarity[] = value.toString().split("::");
			put.add(familyBytes, Bytes.toBytes(songsIdMap.get(itemSimilarity[0])), Bytes.toBytes(itemSimilarity[1]));
		}
		context.write(keyin, put);
	}
}
