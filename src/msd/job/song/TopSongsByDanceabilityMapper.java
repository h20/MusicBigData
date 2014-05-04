package msd.job.song;

import java.io.IOException;

import msd.data.TSVArtistHelper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;

public class TopSongsByDanceabilityMapper extends 
        Mapper<LongWritable, Text, LongWritable, Text> {
    
    LongWritable danceabilityWritable = new LongWritable();
    Text songText = new Text();
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] songData = value.toString().split("\t");
        String filteredData = songData[43];
        String songID = songData[44];
        try {
            if (filteredData.length() > 0 && songID.length() > 0
                    && !filteredData.equals("nan")) {
                songText.set(songID);
                danceabilityWritable.set((long) (Double.parseDouble(filteredData)
                        * 100000l));
                context.write(danceabilityWritable, songText);
            }
        }catch(Exception e) {
            
        }
    }

}
