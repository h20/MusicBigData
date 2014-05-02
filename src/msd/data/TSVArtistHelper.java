package msd.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;

public class TSVArtistHelper {
    
    //public static String TRACK_ID = "trackId";
    public static String ANALYSIS_SAMPLE_RATE = "analysis_sample_rate";
    public static String ARTIST_7DIGITAL_ID = "artist_7digitalid";
    public static String ARTIST_FAMILIARITY = "artist_familiarity";
    public static String ARTIST_HOTTNESS = "artist_hottness";
    public static String ARTIST_ID = "artist_id";
    public static String ARTIST_LATITUDE = "artist_latitude";
    public static String ARTIST_LOCATION = "artist_location";
    public static String ARTIST_LONGITUDE = "artist_longitude";
    public static String ARTIST_MBID = "artist_mbid";
    public static String ARTIST_MBTAG_WORDS = "artist_mbtags_words";
    public static String ARTIST_MBTAG_COUNT = "artist_mbtags_count";
    public static String ARTIST_NAME = "artist_name";
    public static String ARTIST_PLAYMEID = "artist_playmeid";
    public static String ARTIST_TERMS = "artist_terms";
    public static String ARTIST_TERMS_FREQ = "artist_terms_freq";
    public static String ARTIST_TERMS_WEIGHT = "artist_terms_count";
    public static String SIMILAR_ARTISTS = "similar_artists";
    public static String GARBAGE = "---";
    public static int[] artistsFields = {2,3,4,5,6,7,8,9,12,13,14,42};
    public static List<Boolean> bitMap = new ArrayList<Boolean>();
    
    static {
        for (int i = 0; i < 54; ++i) {
            bitMap.add(false);
        }
        for (int num : artistsFields) {
            bitMap.set(num, true);
        }
    }
    public static JSONObject parseLine(String songData) {
        JSONObject map = new JSONObject();
        String [] metadataSplit = songData.split("\t");
        //map.put(TRACK_ID, metadataSplit[0]);
        //map.put(ANALYSIS_SAMPLE_RATE, metadataSplit[1]);
        map.put(ARTIST_7DIGITAL_ID, metadataSplit[2]);
        map.put(ARTIST_FAMILIARITY, metadataSplit[3]);
        map.put(ARTIST_HOTTNESS, metadataSplit[4]);
        map.put(ARTIST_ID, metadataSplit[5]);
        map.put(ARTIST_LATITUDE, metadataSplit[6]);
        map.put(ARTIST_LOCATION, metadataSplit[7]);
        map.put(ARTIST_LONGITUDE, metadataSplit[8]);
        map.put(ARTIST_MBID, metadataSplit[9]);
        //map.put(ARTIST_MBTAG_WORDS, metadataSplit[10]);
        //map.put(ARTIST_MBTAG_COUNT, metadataSplit[11]);
        map.put(ARTIST_NAME, metadataSplit[12]);
        map.put(ARTIST_PLAYMEID, metadataSplit[13]);
        map.put(ARTIST_TERMS, removeLastComma(metadataSplit[14]));
        //map.put(ARTIST_TERMS_FREQ, metadataSplit[15]);
        //map.put(ARTIST_TERMS_WEIGHT, metadataSplit[16]);
        map.put(SIMILAR_ARTISTS, removeLastComma(metadataSplit[42]));
        return map;
    }
    
    public static String removeLastComma(String commaString) {
        if(commaString.length() > 0 &&
                commaString.charAt(commaString.length() - 1) == ',') {
            return commaString.substring(0, commaString.length() - 1);
        }
        return commaString;
    }
    
    public static String getArtistID(String songData) {
        return songData.split("\t")[5];
    }
    
    public static String removeUnusedData(String songData) {
        StringBuilder builder = new StringBuilder();
        String split[] = songData.split("\t");
        for (int i = 0; i < 54; ++i) {
            if (!bitMap.get(i)) {
                split[i] = GARBAGE;
            }
            builder.append(split[i]);
            builder.append("\t");
        }
        return builder.toString();
    }
    
    public static void main(String args[]) throws IOException {
        File file = new File("/home/jeet/Documents/RTBDA/Project/data/A.tsv.a");
        BufferedReader reader = new BufferedReader(new FileReader(file));
        reader.close();
    }

}
