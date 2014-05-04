package msd.job.tags;

import java.io.IOException;

import msd.data.TSVArtistHelper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TagsAndArtistHotnessFilterMapper extends 
        Mapper<LongWritable, Text, Text, DoubleWritable> {
    
    Text artistText = new Text();
    DoubleWritable hotnessWritable = new DoubleWritable();
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String [] songSplit = value.toString().split("\t");
        String artistHotness = songSplit[4];
        String artistTerms[] = TSVArtistHelper.removeLastComma(songSplit[14])
            .split(",");
        if (!artistHotness.equals("") && !artistHotness.equals("nan")) {
            try {
                double hotness = Double.parseDouble(artistHotness);
                for (String artistTerm : artistTerms) {
                    String trimmedArtistTerm = artistTerm.trim();
                    if (trimmedArtistTerm.length() > 0) {
                        artistText.set(trimmedArtistTerm);
                        hotnessWritable.set(hotness);
                        context.write(artistText, hotnessWritable);
                    }
                }
            } catch(Exception ex) {}
        }
    }

}
