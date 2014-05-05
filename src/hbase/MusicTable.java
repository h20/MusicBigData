package hbase;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import msd.data.TSVArtistHelper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.web.resources.GetOpParam;
import org.json.JSONObject;

public class MusicTable {
	
	public static void createRecommendationTable() {
		try {
    		String tableName = "recommendations_large";
    		String [] familys = {"item_based", "ALS", "input"};
    		HBaseHelper.creatTable(tableName, familys);
    	}catch(Exception e){}
	}
	
	public static void createSongSimilarityTable() {
		String tableName = "song_similarity_large";
		String [] familys = {"ratings_cosine", "metadata_cosine"};
		try {
			HBaseHelper.creatTable(tableName, familys);
		} catch (Exception e) {}
	}
	
	public static void createTopSongs() {
	    String tableName = "top_songs";
	    String [] familys = {"most_listened", "most_hot", "most_energy"};
	    try {
            HBaseHelper.creatTable(tableName, familys);
        } catch (Exception e) {}
	}
	
	public static void createTopArtists() {
        String tableName = "top_artists";
        String [] familys = {"most_popular"};
        try {
            HBaseHelper.creatTable(tableName, familys);
        } catch (Exception e) {}
    }
	
	public static void createArtists() {
        String tableName = "artists";
        String [] familys = {"data"};
        try {
            HBaseHelper.creatTable(tableName, familys);
        } catch (Exception e) {}
    }
	
	public static void writeDataForTopSongsNonDFS(
	        String filePath, String family) 
            throws Exception {
        Configuration hbaseConf = HBaseConfiguration.create();
        BufferedReader bufferedReader = new BufferedReader(
                new FileReader(filePath));
        String line = "";
        HTable table = new HTable(hbaseConf, "top_songs");
        int rank = 1;
        List<Put> putList = new ArrayList<Put>();
        while ((line = bufferedReader.readLine()) != null) {
            Put put = new Put(Bytes.toBytes(rank));
            StringTokenizer tokenizer = new StringTokenizer(line);
            String songId = tokenizer.nextToken();
            String count = tokenizer.nextToken();
            if (family.equals("most_hot")) {
                count = String.valueOf(Integer.parseInt(count)/10000.0 - 0.3);
            }
            put.add(Bytes.toBytes(family), 
                    Bytes.toBytes(songId), Bytes.toBytes(count));
            putList.add(put);
            rank++;
        }
        bufferedReader.close();
        table.put(putList);
        table.close();
    }
	
	public static void writeDataForTopSongsDFS(
	            String filePath, String family) 
	        throws Exception {
	    Configuration conf = new Configuration();
	    Configuration hbaseConf = HBaseConfiguration.create();
        FileSystem fileSystem = FileSystem.get(conf);
        FSDataInputStream inputStream = fileSystem.open(new Path(filePath));
        DataInputStream dataInputStream = new DataInputStream(inputStream);
        BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(dataInputStream));
        String line = "";
        HTable table = new HTable(hbaseConf, "top_songs");
        int rank = 1;
        List<Put> putList = new ArrayList<Put>();
        while ((line = bufferedReader.readLine()) != null) {
            Put put = new Put(Bytes.toBytes(rank));
            StringTokenizer tokenizer = new StringTokenizer(line);
            String songId = tokenizer.nextToken();
            String count = tokenizer.nextToken();
            put.add(Bytes.toBytes(family), 
                    Bytes.toBytes(songId), Bytes.toBytes(count));
            putList.add(put);
            rank++;
        }
        bufferedReader.close();
        dataInputStream.close();
        inputStream.close();
        fileSystem.close();
        table.put(putList);
        table.close();
	}
	
	public static void writeDataForTopArtistsDFS(String filePath) 
            throws Exception {
        Configuration conf = new Configuration();
        Configuration hbaseConf = HBaseConfiguration.create();
        FileSystem fileSystem = FileSystem.get(conf);
        FSDataInputStream inputStream = fileSystem.open(new Path(filePath));
        DataInputStream dataInputStream = new DataInputStream(inputStream);
        BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(dataInputStream));
        String line = "";
        HTable table = new HTable(hbaseConf, "top_artists");
        int rank = 1;
        List<Put> putList = new ArrayList<Put>();
        while ((line = bufferedReader.readLine()) != null) {
            Put put = new Put(Bytes.toBytes(rank));
            JSONObject json = new JSONObject(line);
            String artistId = json.getString(TSVArtistHelper.ARTIST_ID);
            put.add(Bytes.toBytes("most_popular"), 
                Bytes.toBytes(TSVArtistHelper.ARTIST_ID), 
                    Bytes.toBytes(artistId));
            putList.add(put);
            rank++;
        }
        bufferedReader.close();
        dataInputStream.close();
        inputStream.close();
        fileSystem.close();
        table.put(putList);
        table.close();
    }
	
	public static void writeDataForTopArtistsNonDFS(String filePath) 
            throws Exception {
        //Configuration conf = new Configuration();
        Configuration hbaseConf = HBaseConfiguration.create();
        //FileSystem fileSystem = FileSystem.get(conf);
        //FSDataInputStream inputStream = fileSystem.open(new Path(filePath));
        FileReader inputStream = new FileReader(filePath);
        /*DataInputStream dataInputStream = new DataInputStream(inputStream);
        BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(dataInputStream));*/
        BufferedReader bufferedReader = new BufferedReader(inputStream);
        String line = "";
        HTable table = new HTable(hbaseConf, "top_artists");
        int rank = 1;
        List<Put> putList = new ArrayList<Put>();
        while ((line = bufferedReader.readLine()) != null) {
            Put put = new Put(Bytes.toBytes(rank));
            JSONObject json = new JSONObject(line);
            String artistId = json.getString(TSVArtistHelper.ARTIST_ID);
            put.add(Bytes.toBytes("most_popular"), 
                Bytes.toBytes(TSVArtistHelper.ARTIST_ID), 
                    Bytes.toBytes(artistId));
            putList.add(put);
            rank++;
        }
        bufferedReader.close();
        //dataInputStream.close();
        inputStream.close();
        //fileSystem.close();
        table.put(putList);
        table.close();
    }
	
	public static void writeDataInArtistsTableDFS(String filePath) 
            throws Exception {
        Configuration conf = new Configuration();
        Configuration hbaseConf = HBaseConfiguration.create();
        FileSystem fileSystem = FileSystem.get(conf);
        FSDataInputStream inputStream = fileSystem.open(new Path(filePath));
        DataInputStream dataInputStream = new DataInputStream(inputStream);
        BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(dataInputStream));
        String line = "";
        HTable table = new HTable(hbaseConf, "artists");
        List<Put> putList = new ArrayList<Put>();
        while ((line = bufferedReader.readLine()) != null) {
            JSONObject json = new JSONObject(line);
            String artistId = json.getString(TSVArtistHelper.ARTIST_ID);
            Put put = new Put(Bytes.toBytes(artistId));
            addToPut(put, "data", json, TSVArtistHelper.ARTIST_7DIGITAL_ID);
            addToPut(put, "data", json, TSVArtistHelper.ARTIST_FAMILIARITY);
            addToPut(put, "data", json, TSVArtistHelper.ARTIST_HOTTNESS);
            addToPut(put, "data", json, TSVArtistHelper.ARTIST_LATITUDE);
            addToPut(put, "data", json, TSVArtistHelper.ARTIST_LOCATION);
            addToPut(put, "data", json, TSVArtistHelper.ARTIST_LONGITUDE);
            addToPut(put, "data", json, TSVArtistHelper.ARTIST_MBID);
            addToPut(put, "data", json, TSVArtistHelper.ARTIST_NAME);
            addToPut(put, "data", json, TSVArtistHelper.ARTIST_PLAYMEID);
            addToPut(put, "data", json, TSVArtistHelper.ARTIST_TERMS);
            addToPut(put, "data", json, TSVArtistHelper.SIMILAR_ARTISTS);
            putList.add(put);
            if (putList.size() % 1000 == 0) {
                table.put(putList);
                putList.clear();
            }
        }
        bufferedReader.close();
        dataInputStream.close();
        inputStream.close();
        fileSystem.close();
        if (putList.size() > 0) {
            table.put(putList);
        }
        table.close();
    }
	
	public static void writeDataInArtistsTableNonDFS(String filePath) 
            throws Exception {
        Configuration hbaseConf = HBaseConfiguration.create();
        BufferedReader bufferedReader = new BufferedReader(
                new FileReader(filePath));
        String line = "";
        HTable table = new HTable(hbaseConf, "artists");
        List<Put> putList = new ArrayList<Put>();
        while ((line = bufferedReader.readLine()) != null) {
            JSONObject json = new JSONObject(line);
            String artistId = json.getString(TSVArtistHelper.ARTIST_ID);
            Put put = new Put(Bytes.toBytes(artistId));
            addToPut(put, "data", json, TSVArtistHelper.ARTIST_7DIGITAL_ID);
            addToPut(put, "data", json, TSVArtistHelper.ARTIST_FAMILIARITY);
            addToPut(put, "data", json, TSVArtistHelper.ARTIST_HOTTNESS);
            addToPut(put, "data", json, TSVArtistHelper.ARTIST_LATITUDE);
            addToPut(put, "data", json, TSVArtistHelper.ARTIST_LOCATION);
            addToPut(put, "data", json, TSVArtistHelper.ARTIST_LONGITUDE);
            addToPut(put, "data", json, TSVArtistHelper.ARTIST_MBID);
            addToPut(put, "data", json, TSVArtistHelper.ARTIST_NAME);
            addToPut(put, "data", json, TSVArtistHelper.ARTIST_PLAYMEID);
            addToPut(put, "data", json, TSVArtistHelper.ARTIST_TERMS);
            addToPut(put, "data", json, TSVArtistHelper.SIMILAR_ARTISTS);
            putList.add(put);
            if (putList.size() % 1000 == 0) {
                table.put(putList);
                putList.clear();
            }
        }
        bufferedReader.close();
        if (putList.size() > 0) {
            table.put(putList);
        }
        table.close();
    }
	
	private static void addToPut(Put put,
	        String family, JSONObject json, String jsonKey) {
	    if (!json.isNull(jsonKey)) {
	        put.add(Bytes.toBytes(family), 
                Bytes.toBytes(jsonKey), 
                    Bytes.toBytes(json.getString(jsonKey)));
	    }
	}
	
	private static void createTags(String path) throws IOException {
	    BufferedReader bufferedReader = new BufferedReader(new FileReader(path));
	    FileWriter writer = new FileWriter(path+"_output");
	    String line = "";
	    Map<String, Integer> tagMap = new HashMap<String, Integer>();
	    int max = 0;
	    int tagCount = 200;
	    while ((line = bufferedReader.readLine()) != null &&
	            tagCount > 0) {
	        String[] split = line.split("\t");
	        String tag = split[0];
	        int count = Integer.parseInt(split[1]);
	        if (max == 0) {
	            max = count;
	        }
	        count = (int) ((count/(max*1.0)) * 30);
	        tagMap.put(tag, count);
	        tagCount--;
	    }
	    for (Map.Entry<String, Integer> entry : tagMap.entrySet()) {
	        for (int i = 0; i < entry.getValue(); ++i) {
	            writer.append(entry.getKey());
	            writer.append('\n');
	        }
	    }
	    writer.flush();
	    bufferedReader.close();
	    writer.close();
	}
	
	public static void main(String args[]) throws Exception {
	    /*writeDataForTopSongsNonDFS("/home/jeet/Documents/RTBDA/Project/data/AWS_Top_Songs/part-r-00000", 
	            "most_hot");*/
	    //HBaseHelper.getOneRecord("top_songs", 1);
	    //createTags("/home/jeet/Documents/RTBDA/Project/data/AWS_Tags_Count/part-r-00000");
	    createSongSimilarityTable();
	}
}
