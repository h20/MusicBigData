package recommendations;

public class MusicTable {
	
	public static void createRecommendationTable() {
		try {
    		String tableName = "recommendations";
    		String [] familys = {"item_based", "ALS", "input"};
    		HBaseTest.creatTable(tableName, familys);
    	}catch(Exception e){}
	}
}
