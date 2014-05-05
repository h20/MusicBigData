package msd.job.tags;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import msd.data.TSVArtistHelper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SongsCountByYearMapper 
    extends Mapper<LongWritable, Text, Text, LongWritable> {
    
    private Text yearTerm = new Text();
    private LongWritable ONE = new LongWritable(1);
    
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
        String [] songSplit = value.toString().split("\t");
        String year = songSplit[53];
        String artistTerms[] = TSVArtistHelper.removeLastComma(songSplit[14])
            .split(",");
        try {
            for (String artistTerm : artistTerms) {
                String trimmedArtistTerm = artistTerm.trim();
                if (trimmedArtistTerm.length() > 0 && year.length() > 0
                        && !year.equals("nan") 
                            && topTerms.contains(trimmedArtistTerm)) {
                    yearTerm.set(year + "\t" + trimmedArtistTerm);
                    context.write(yearTerm, ONE);
                }
            }
        } catch(Exception ex) {}
    }
}
