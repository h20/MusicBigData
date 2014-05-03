package recommendations;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import msd.data.TSVArtistHelper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hdfs.web.resources.GetOpParam;
import org.json.JSONObject;

public class MusicTable {
	
	public static void createRecommendationTable() {
		try {
    		String tableName = "recommendations";
    		String [] familys = {"item_based", "ALS", "input"};
    		HBaseTest.creatTable(tableName, familys);
    	}catch(Exception e){}
	}
	
	public static void createSongSimilarityTable() {
		String tableName = "song_similarity";
		String [] familys = {"ratings_cosine", "metadata_cosine"};
		try {
			HBaseTest.creatTable(tableName, familys);
		} catch (Exception e) {}
	}
	
	public static void createTopSongs() {
	    String tableName = "top_songs";
	    String [] familys = {"most_listened", "most_hot", "most_energy"};
	    try {
            HBaseTest.creatTable(tableName, familys);
        } catch (Exception e) {}
	}
	
	public static void createTopArtists() {
        String tableName = "top_artists";
        String [] familys = {"most_popular"};
        try {
            HBaseTest.creatTable(tableName, familys);
        } catch (Exception e) {}
    }
	
	public static void createArtists() {
        String tableName = "artists";
        String [] familys = {"data"};
        try {
            HBaseTest.creatTable(tableName, familys);
        } catch (Exception e) {}
    }
	
	public static void writeDataForMostListenedSongs(String filePath) 
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
            put.add(Bytes.toBytes("most_listened"), 
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
	
	public static void main(String args[]) throws Exception {
	}
}
