package recommendations;

public class MusicTable {
	
	public void createRecommendationTable() {
		try {
    		String tableName = "recommendations";
    		String [] familys = {"item_based", "ALS"};
    		HBaseTest.creatTable(tableName, familys);
    	}catch(Exception e){}
	}
}
