package recommendations.job.hbase;

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

public class RecommendationInsertReducer extends TableReducer<Text, Text, Text>  {
	
	private byte[] rawUpdateColumnFamily = Bytes.toBytes("item_based");
	
	static Map<Integer, String> usersIdMap;
	static Map<String, String> songsIdMap;
	
	public static Map<Integer, String> getIdMapFromFileUser(FileSystem fileSystem, Path path) throws IOException {
		if (fileSystem.exists(path)) {
			FSDataInputStream in = fileSystem.open(path);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			Map<Integer, String> map = new HashMap<Integer, String>();
			String line;
			int id = 0;
			while((line = br.readLine()) != null) {
				map.put(id++, line.trim());
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
			int id = 0;
            while((line = br.readLine()) != null) {
                map.put(String.valueOf(id++), line.trim());
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
		Path usersPath = new Path("/user/hduser/reco_users_large/part-r-00000");
		Path songsPath = new Path("/user/hduser/reco_songs_large/part-r-00000");
		
		if (usersIdMap == null) {
			usersIdMap = getIdMapFromFileUser(fileSystem, usersPath);
		}
		
		if (songsIdMap == null) {
			songsIdMap = getIdMapFromFileSong(fileSystem, songsPath);
		}
	for (Text val : values) {
	   // put date in table
	   String value = val.toString();
	   value = value.substring(1, value.length() - 1);
	   String [] itemRecos = value.split(",");
	   String originalUserIdString = null;
	   try {
		   originalUserIdString = usersIdMap.get(Integer.parseInt(keyin.toString()));
	   } catch(Exception e) {
		   System.out.println(keyin.toString());
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
			   context.write(keyin, put);
		   }
	   }
	}
  }
}
