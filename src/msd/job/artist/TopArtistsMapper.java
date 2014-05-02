package msd.job.artist;

import java.io.IOException;

import msd.data.TSVArtistHelper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;

public class TopArtistsMapper extends 
        Mapper<LongWritable, Text, LongWritable, Text> {
    
    LongWritable popularityWritable = new LongWritable();
    Text artistData = new Text();
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        JSONObject songMap = TSVArtistHelper.parseLine(value.toString());
        String familiarity = (String) songMap.get(
                TSVArtistHelper.ARTIST_FAMILIARITY);
        String hottness = (String) songMap.get(
                TSVArtistHelper.ARTIST_HOTTNESS);
        if (familiarity != null && hottness != null) {
            long popularity = (long) ((Double.parseDouble(familiarity) + 
                    Double.parseDouble(hottness)) * 1000000);
            artistData.set(songMap.toString());
            popularityWritable.set(popularity);
            context.write(popularityWritable, artistData);
        }
    }

}
