package recommendations;

public class MusicTable {
	
	public static void createRecommendationTable() {
		try {
    		String tableName = "recommendations";
    		String [] familys = {"item_based", "ALS", "input"};
    		HBaseTest.creatTable(tableName, familys);
    	}catch(Exception e){}
	}
	
	public static void createSongSimilarityTable() {
		String tableName = "song_similarity";
		String [] familys = {"ratings_cosine", "metadata_cosine"};
		try {
			HBaseTest.creatTable(tableName, familys);
		} catch (Exception e) {}
	}
	
	public static void main(String args[]) throws Exception {
		HBaseTest.getOneRecord("song_similarity", "SOVHZBK12AF72A66E8");
	}
}
