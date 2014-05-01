package ratings.job;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TopSongsJobStarter {
    public static void main(String args[]) 
            throws ClassNotFoundException, IOException, InterruptedException {
        
        long startTime = System.currentTimeMillis();
        String[] argsMostListened = new String[2];
        
        //input directory
        argsMostListened[0] = args[0];
        
        //temp directory
        argsMostListened[1] = args[1];
        
        MostListenedSongsJob.main(argsMostListened);
        
        String [] argsTopListened = new String[3];
        
        //temp directory
        argsTopListened[0] = args[1];
        
        //final directory
        argsTopListened[1] = args[2];
        
        //max count
        if (args.length == 4) {
            argsTopListened[2] = args[3];
        }
        
        TopListenedSongsSortJob.main(argsTopListened);
        
        Path tempPath = new Path(args[1]);
        
        //Cleaning up the temp directory
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(tempPath)) {
            fileSystem.delete(tempPath, true);
        }
        long endTime = System.currentTimeMillis();
        System.out.println((endTime - startTime)/1000.0 + " seconds.");
    }
}
