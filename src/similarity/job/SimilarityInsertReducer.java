package similarity.job;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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
		Path songsPath = new Path("/user/hduser/reco_songs_large/part-r-00000");
		
		if (songsIdMap == null) {
			songsIdMap = getIdMapFromFileSong(fileSystem, songsPath);
		}
		if (familyBytes == null) {
			familyBytes = Bytes.toBytes(context.getConfiguration().get("familyBytes"));
		}
		Put put = new Put(Bytes.toBytes(songsIdMap.get(keyin.toString())));
		TreeMap<Double, List<String>> topSimilar = 
		        new TreeMap<Double, List<String>>(Collections.reverseOrder());
        for (Text value : values) {
            String itemSimilarity[] = value.toString().split("::");
            double similarity = Double.parseDouble(itemSimilarity[1]);
            List<String> songs = topSimilar.get(similarity);
            if (songs == null) {
                songs = new ArrayList<String>();
                topSimilar.put(similarity, songs);
            }
            songs.add(itemSimilarity[0]);
        }
        
        Iterator<Map.Entry<Double, List<String>>> mapIter = 
                topSimilar.entrySet().iterator();
        int maxCount = 15;
		while(mapIter.hasNext() && maxCount > 0) {
		    Map.Entry<Double, List<String>> entry = mapIter.next();
		    double key = entry.getKey();
		    List<String> list = entry.getValue();
		    for (int i = 0; i < list.size() && maxCount > 0; i++) {
		        put.add(familyBytes, Bytes.toBytes(songsIdMap.get(list.get(i))), Bytes.toBytes(String.valueOf(key)));
	            maxCount--;
		    }
		}
		context.write(keyin, put);
	}
}
