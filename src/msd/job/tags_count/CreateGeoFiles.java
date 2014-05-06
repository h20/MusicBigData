package msd.job.tags_count;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class CreateGeoFiles {
    
public static void main(String args[]) throws IOException {
        
        String basePath = "/home/jeet/Documents/RTBDA/Project/data/AWS_TOP_TAGS_LOCATION_COUNT/";
        String file = "/home/jeet/Documents/RTBDA/Project/data/AWS_TOP_TAGS_LOCATION_COUNT/part-r-00000";
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line = "";
        String tagName = "";
        FileWriter writer = null; 
        while ((line = reader.readLine()) != null) {
            String split[] = line.split("\t");
            if (tagName.length() == 0 || !tagName.equals(split[0])) {
                tagName = split[0];
                if (writer != null) {
                    writer.close();
                }
                writer = new FileWriter(basePath+tagName);
            }
            writer.append(line);
            writer.append("\n");
        }
        reader.close();
    } 

}
