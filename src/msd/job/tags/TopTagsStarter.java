package msd.job.tags;

public class TopTagsStarter {
    public static void main(String args[]) throws Exception {
        long startTime = System.currentTimeMillis();
        String[] argsFilter = new String[2];
        
        //input directory
        argsFilter[0] = args[0];
        
        //temp directory
        argsFilter[1] = args[1];
        
        TagsAndArtistHotnessFilterJob.main(argsFilter);
        
        String [] argsSort = new String[2];
        
        //temp directory
        argsSort[0] = args[1];
        
        //final directory
        argsSort[1] = args[2];
        
        SortTagsByHotnessJob.main(argsSort);
        
        long endTime = System.currentTimeMillis();
        System.out.println((endTime - startTime)/1000.0 + " seconds.");
    }
}
