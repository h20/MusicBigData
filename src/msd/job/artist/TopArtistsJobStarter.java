package msd.job.artist;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TopArtistsJobStarter {
    
    public static void main(String args[]) throws Exception {
        long startTime = System.currentTimeMillis();
        String[] filterInput = new String[2];
        
        //input directory
        filterInput[0] = args[0];
        
        //temp directory
        filterInput[1] = args[1];
        
        FilterArtistJob.main(filterInput);
        
        String [] argsTopArtists = new String[args.length - 1];
        
        //temp directory
        argsTopArtists[0] = args[1];
        
        //final directory
        argsTopArtists[1] = args[2];
        
        //max count
        if (args.length == 4) {
            argsTopArtists[2] = args[3];
        }
        
        TopArtistsJob.main(argsTopArtists);
        
        /*Path tempPath = new Path(args[1]);
        
        //Cleaning up the temp directory
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(tempPath)) {
            fileSystem.delete(tempPath, true);
        }*/
        long endTime = System.currentTimeMillis();
        System.out.println("Time: "+(endTime - startTime)/1000.0 + " seconds.");
    }

}
