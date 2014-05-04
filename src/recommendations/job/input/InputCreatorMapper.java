package recommendations.job.input;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InputCreatorMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	static Map<String, Integer> usersIdMap;
	static Map<String, String> songsIdMap;
	
	public static Map<String, Integer> getIdMapFromFileUser(FileSystem fileSystem, Path path) throws IOException {
		if (fileSystem.exists(path)) {
			FSDataInputStream in = fileSystem.open(path);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			Map<String, Integer> map = new HashMap<String, Integer>();
			String line;
			int id = 0;
			while((line = br.readLine()) != null) {
				map.put(line.trim(), id++);
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
				map.put(values[0], values[1]);
			}
			br.close();
			in.close();
			return map;
		}
		return null;
	}
	
	public static Map<String, Integer> getIdMapFromFileUser(
	        FileInputStream in, Path path) throws IOException {
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            Map<String, Integer> map = new HashMap<String, Integer>();
            String line;
            int id = 0;
            while((line = br.readLine()) != null) {
                map.put(line.trim(), id++);
            }
            br.close();
            in.close();
            return map;
    }
    
    public static Map<String, String> getIdMapFromFileSong(
            FileInputStream in, Path path) throws IOException {
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            Map<String, String> map = new HashMap<String, String>();
            String line;
            int id = 0;
            while((line = br.readLine()) != null) {
                map.put(line.trim(), String.valueOf(id++));
            }
            br.close();
            in.close();
            return map;
    }
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    /*context.getConfiguration().get*/
		//FileSystem fileSystem = FileSystem.get(context.getConfiguration());
		//FileInputStream fstream = new FileInputStream(s3FilePath.toString());
		/*Path usersPath = new Path("/user/hduser/music_users_small");
		Path songsPath = new Path("/user/hduser/music_songs_small");*/
		
		Path usersPath = DistributedCache.getLocalCacheFiles(
                context.getConfiguration())[0];
        Path songsPath = DistributedCache.getLocalCacheFiles(
                context.getConfiguration())[1];
		
		/*if (usersIdMap == null) {
			usersIdMap = getIdMapFromFileUser(fileSystem, usersPath);
		}
		
		if (songsIdMap == null) {
			songsIdMap = getIdMapFromFileSong(fileSystem, songsPath);
		}*/
		
		if (usersIdMap == null) {
            usersIdMap = getIdMapFromFileUser(
                new FileInputStream(usersPath.toString()), usersPath);
        }
        
        if (songsIdMap == null) {
            songsIdMap = getIdMapFromFileSong(
                new FileInputStream(songsPath.toString()), songsPath);
        }
		
		String line = value.toString();
	    StringTokenizer tokenizer = new StringTokenizer(line);
	    StringBuilder lineBuilder = new StringBuilder();
	    String userId = tokenizer.nextToken();
	    String item = tokenizer.nextToken();
	    String rating = tokenizer.nextToken();
	    Integer userKey = usersIdMap.get(userId);
	    String itemKey =songsIdMap.get(item);
	    if (userKey != null && itemKey != null && rating != null) {
		    lineBuilder.append(userKey);
		    lineBuilder.append(",");
		    lineBuilder.append(itemKey);
		    lineBuilder.append(",");
		    lineBuilder.append(rating);
	    }
	    context.write(new Text(lineBuilder.toString()), new Text());
    }
}
