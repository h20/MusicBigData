package msd.job.tags_count;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import msd.data.TSVArtistHelper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SongsCountByGeoMapper 
    extends Mapper<LongWritable, Text, Text, Text> {
    
    private Text geoTerm = new Text();
    private Text locationText = new Text();
    
    @SuppressWarnings("serial")
    private static final Set<String> topTerms = new HashSet<String>() {{
        add("rock");
        add("electronic");
        add("pop");
        add("alternative rock");
        add("jazz");
        add("hip hop");
        add("united states");
        add("alternative");
        add("pop rock");
        add("indie");
        add("folk");
        add(" punk");
        add("indie rock");
        add("house");
        add("experimental");
        add("soul");
        add("electro");
        add("downtempo");
        add("blues");
        add("techno");
    }};
    
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        try {
        String [] songSplit = value.toString().split("\t");
        double lattitude = Double.parseDouble(songSplit[6]);
        String location = songSplit[7].toLowerCase();
        double longitude = Double.parseDouble(songSplit[8]);
        
        String artistTerms[] = TSVArtistHelper.removeLastComma(songSplit[14])
            .split(",");
            for (String artistTerm : artistTerms) {
                String trimmedArtistTerm = artistTerm.trim();
                if (topTerms.contains(trimmedArtistTerm) && 
                        trimmedArtistTerm.length() > 0 
                              && location.length() > 0) {
                    StringBuilder builder = new StringBuilder();
                    builder.append(artistTerm).append("\t").append(lattitude)
                        .append("\t").append(longitude);
                    locationText.set(location);
                    geoTerm.set(builder.toString());
                    context.write(geoTerm, locationText);
                }
            }
        } catch(Exception ex) {}
    }
}
