package msd.job.artist;

import java.io.IOException;

import msd.data.TSVArtistHelper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FilterArtistMapper extends 
        Mapper<LongWritable, Text, Text, Text> {
    
    Text artistText = new Text();
    Text filteredText = new Text();
    
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String songData = value.toString();
        String filteredData = TSVArtistHelper.removeUnusedData(songData);
        String artistID = TSVArtistHelper.getArtistID(filteredData);
        artistText.set(artistID);
        filteredText.set(filteredData);
        context.write(artistText, filteredText);
    }
}
