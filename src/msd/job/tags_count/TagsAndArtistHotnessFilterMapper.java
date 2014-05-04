package msd.job.tags_count;

import java.io.IOException;

import msd.data.TSVArtistHelper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TagsAndArtistHotnessFilterMapper extends 
        Mapper<LongWritable, Text, Text, LongWritable> {
    
    Text artistText = new Text();
    LongWritable hotnessWritable = new LongWritable();
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String [] songSplit = value.toString().split("\t");
        String artistTerms[] = TSVArtistHelper.removeLastComma(songSplit[14])
            .split(",");
            try {
                for (String artistTerm : artistTerms) {
                    String trimmedArtistTerm = artistTerm.trim();
                    if (trimmedArtistTerm.length() > 0) {
                        artistText.set(trimmedArtistTerm);
                        hotnessWritable.set(1l);
                        context.write(artistText, hotnessWritable);
                    }
                }
            } catch(Exception ex) {}
    }

}
