package recommendations.job.hbase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RecommendationsInsertHbaseMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
	private byte[] rawUpdateColumnFamily = Bytes.toBytes("item_based");
	
	public static Map<Integer, String> getIdMapFromFileUser(FileSystem fileSystem, Path path) throws IOException {
		if (fileSystem.exists(path)) {
			FSDataInputStream in = fileSystem.open(path);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			Map<Integer, String> map = new HashMap<Integer, String>();
			String line;
			int id = 0;
			while((line = br.readLine()) != null) {
				map.put(id++, line);
			}
			br.close();
			in.close();
			return map;
		}
		return null;
	}
	
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
	
	static Map<Integer, String> usersIdMap;
	static Map<String, String> songsIdMap;
	
	public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {  
		StringTokenizer tokenizer = new StringTokenizer(val.toString());
	    String keyin = tokenizer.nextToken();
	    String value = tokenizer.nextToken();
	    FileSystem fileSystem = FileSystem.get(context.getConfiguration());
		Path usersPath = new Path("/user/hduser/music_users_small");
		Path songsPath = new Path("/user/hduser/music_songs_small");
		
		if (usersIdMap == null) {
			usersIdMap = getIdMapFromFileUser(fileSystem, usersPath);
		}
		
		if (songsIdMap == null) {
			songsIdMap = getIdMapFromFileSong(fileSystem, songsPath);
		}
		
	   value = value.substring(1, value.length() - 1);
	   String [] itemRecos = value.split(",");
	   String originalUserIdString = null;
	   try {
		   originalUserIdString = usersIdMap.get(Integer.parseInt(keyin.toString()));
	   } catch(Exception e) {
		   
	   }
	   if (originalUserIdString != null) {
		   Put put = new Put(originalUserIdString.getBytes());
		   for (String itemReco : itemRecos) {
			   String [] itemRecoSplit = itemReco.split(":");
			   String item = itemRecoSplit[0];
			   String rating = itemRecoSplit[1];
			   String originalItemIdString = songsIdMap.get(item);
			   if (originalItemIdString != null && rating != null) {
				   put.add(rawUpdateColumnFamily, Bytes.toBytes(originalItemIdString), Bytes.toBytes(rating));
			   }
		   }
		   if (put.size() > 0) {
			   ImmutableBytesWritable HKey = new ImmutableBytesWritable(Bytes.toBytes(originalUserIdString));
			   context.write(HKey, put);
		   }
	   }
	    
	}
}
