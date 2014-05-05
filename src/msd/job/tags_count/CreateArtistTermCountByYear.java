package msd.job.tags_count;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

public class CreateArtistTermCountByYear {
    
    public static int getDecade(int year) {
        int base = 1920;
        for (int i = 0; i <= 8; ++i) {
            if (year >= (base + i*10) && year < (base + (i+1)*10)) {
                return (base + i*10);
            }
        }
        return 0;
    }
    
    public static void main(String args[]) throws IOException {
        
        String basePath = "/home/jeet/Documents/RTBDA/Project/data/AWS_TAGS_YEAR_COUNT/";
        String file = "/home/jeet/Documents/RTBDA/Project/data/AWS_TAGS_YEAR_COUNT/part-r-00000";
        Map<String, TreeMap<Integer, Integer>> tagsYearCountMap = 
                new HashMap<String, TreeMap<Integer, Integer>>();
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line = "";
        while ((line = reader.readLine()) != null) {
            String split[] = line.split("\t");
            Integer year = Integer.parseInt(split[0].trim());
            String term = split[1].trim();
            Integer count = Integer.parseInt(split[2].trim());
            if (year != 0 && year < 2010) {
                TreeMap<Integer, Integer> yearCountMap = tagsYearCountMap
                    .get(term);
                if (yearCountMap == null) {
                    yearCountMap = new TreeMap<Integer, Integer>();
                    tagsYearCountMap.put(term, yearCountMap);
                }
                int decade = getDecade(year);
                if (decade != 0) {
                    Integer oldCount = yearCountMap.get(decade);
                    if (oldCount == null) {
                        oldCount = 0;
                    }
                    yearCountMap.put(decade, oldCount.intValue() + count);
                }
            }
        }
        reader.close();
        //System.out.println(tagsYearCountMap.get("rock"));
        Iterator<Entry<String, TreeMap<Integer, Integer>>> tagsYearCountIter 
            = tagsYearCountMap.entrySet().iterator();
        while(tagsYearCountIter.hasNext()) {
            Entry<String, TreeMap<Integer, Integer>> entry = tagsYearCountIter.next();
            String tag = entry.getKey();
            String tagFilePath = basePath + tag;
            TreeMap<Integer, Integer> yearCountMap = entry.getValue();
            FileWriter fileW = new FileWriter(tagFilePath);
            Set<Entry<Integer, Integer>> countMapEntrySet = yearCountMap.entrySet();
            for (Entry<Integer, Integer> countMapEntry : countMapEntrySet) {
                fileW.append(countMapEntry.getKey() + "s");
                fileW.append(":");
                fileW.append(String.valueOf(countMapEntry.getValue()));
                fileW.append("\n");
            }
            fileW.close();
        }
    } 

}
