package hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
 
public class HBaseHelper {
 
    private static Configuration conf = null;
    /**
     * Initialization
     */
    static {
        conf = HBaseConfiguration.create();
    }
 
    /**
     * Create a table
     */
    public static void creatTable(String tableName, String[] familys)
            throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (admin.tableExists(tableName)) {
            System.out.println("table already exists!");
        } else {
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            for (int i = 0; i < familys.length; i++) {
                tableDesc.addFamily(new HColumnDescriptor(familys[i]));
            }
            admin.createTable(tableDesc);
            System.out.println("create table " + tableName + " ok.");
        }
        admin.close();
    }
 
    /**
     * Delete a table
     */
    public static void deleteTable(String tableName) throws Exception {
        try {
            HBaseAdmin admin = new HBaseAdmin(conf);
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("delete table " + tableName + " ok.");
            admin.close();
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        }
        
    }
 
    /**
     * Put (or insert) a row
     */
    public static void addRecord(String tableName, String rowKey,
            String family, String qualifier, String value) throws Exception {
        try {
            HTable table = new HTable(conf, tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes
                    .toBytes(value));
            table.put(put);
            System.out.println("insert recored " + rowKey + " to table "
                    + tableName + " ok.");
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
 
    /**
     * Delete a row
     */
    public static void delRecord(String tableName, String rowKey)
            throws IOException {
        HTable table = new HTable(conf, tableName);
        List<Delete> list = new ArrayList<Delete>();
        Delete del = new Delete(rowKey.getBytes());
        list.add(del);
        table.delete(list);
        System.out.println("del recored " + rowKey + " ok.");
        table.close();
    }
 
    /**
     * Get a row
     */
    public static void getOneRecord (String tableName, String rowKey) throws IOException{
        HTable table = new HTable(conf, tableName);
        Get get = new Get(rowKey.getBytes());
        Result rs = table.get(get);
        for(KeyValue kv : rs.raw()){
            System.out.print(new String(kv.getRow()) + " " );
            System.out.print(new String(kv.getFamily()) + ":" );
            System.out.print(new String(kv.getQualifier()) + " " );
            System.out.print(kv.getTimestamp() + " " );
            System.out.println(new String(kv.getValue()));
        }
        table.close();
    }
    
    public static void getOneRecord(String tableName, int rowKey) throws IOException{
        HTable table = new HTable(conf, tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        Result rs = table.get(get);
        for(KeyValue kv : rs.raw()){
            System.out.print(Bytes.toInt(rs.getRow()) + " ");
            System.out.print(Bytes.toString(kv.getFamily()) + ":" );
            System.out.print(Bytes.toString(kv.getQualifier()) + " " );
            System.out.print(kv.getTimestamp() + " " );
            System.out.println(Bytes.toString(kv.getValue()));
        }
        table.close();
    }
    
    /**
     * Scan (or list) a table
     */
    public static void getAllRecord (String tableName) {
        try{
             HTable table = new HTable(conf, tableName);
             Scan s = new Scan();
             ResultScanner ss = table.getScanner(s);
             for(Result r:ss){
                 for(KeyValue kv : r.raw()){
                    System.out.print(new String(kv.getRow()) + " ");
                    System.out.print(new String(kv.getFamily()) + ":");
                    System.out.print(new String(kv.getQualifier()) + " ");
                    System.out.print(kv.getTimestamp() + " ");
                    System.out.println(new String(kv.getValue()));
                 }
             }
             table.close();
        } catch (IOException e){
            e.printStackTrace();
        }
    }
    
    /*public static void main(String args[]) {
    	String value = "1	[10:5.0,18:5.0,4:5.0,8:5.0,11:5.0,17:5.0,2:5.0,3:5.0,7:5.0,20:5.0]";
    	String line = value.toString();
	    StringTokenizer tokenizer = new StringTokenizer(line);
	    System.out.println(tokenizer.nextToken());
	    System.out.println(tokenizer.nextToken());
    	getAllRecord("movie_recommendations");
    }*/
    /*public static void main(String argsp[]) {
    	try {
    		String tableName = "recommendations";
    		String [] familys = {"item_based", "ALS"};
    		HBaseTest.creatTable(tableName, familys);
    	}catch(Exception e){}
    }*/
    public static void main1(String[] agrs) {
        try {
            String tablename = "scores";
            String[] familys = { "grade", "course" };
            /*HBaseTest.creatTable(tablename, familys);*/
 
            // add record zkb
            /*HBaseTest.addRecord(tablename, "zkb", "grade", "", "5");
            HBaseTest.addRecord(tablename, "zkb", "course", "", "90");
            HBaseTest.addRecord(tablename, "zkb", "course", "math", "97");
            HBaseTest.addRecord(tablename, "zkb", "course", "art", "87");
            // add record baoniu
            HBaseTest.addRecord(tablename, "baoniu", "grade", "", "4");
            HBaseTest.addRecord(tablename, "baoniu", "course", "math", "89");*/
 
            System.out.println("===========get one record========");
            HBaseHelper.getOneRecord(tablename, "zkb");
 
            System.out.println("===========show all record========");
            HBaseHelper.getAllRecord(tablename);
 
            System.out.println("===========del one record========");
            /*HBaseTest.delRecord(tablename, "baoniu");*/
            HBaseHelper.getAllRecord(tablename);
 
            System.out.println("===========show all record========");
            HBaseHelper.getAllRecord(tablename);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static void main(String args[]) throws Exception {
    	/*String tableName = "recommendations";
		String [] familys = {"item_based", "ALS", "input"};
		HBaseTest.creatTable(tableName, familys);*/
    	//MusicTable.createRecommendationTable();
    	getOneRecord("recommendations", "fd50c4007b68a3737fe052d5a4f78ce8aa117f3d");
    	/*HTable table = new HTable(conf, "movie_recommendations");
    	Put put = new Put("fd50c4007b68a3737fe052d5a4f78ce8aa117f3d".getBytes());
    	put.add("input".getBytes(), "SOAAADD12AB018A9DD".getBytes(), Bytes.toBytes("0"));
    	table.put(put);
    	table.close();*/
    	//deleteTable("movie_recommendations");
    	//getAllRecord("movie_recommendations");
    	//String tableName = "movie_recommendations";
		//String [] familys = {"item_based", "ALS", "input"};
		//deleteTable(tableName);
		//creatTable(tableName, familys);
		/*HBaseAdmin admin = new HBaseAdmin(conf);
		admin.addColumn(Bytes.toBytes(tableName), new HColumnDescriptor("input"));
		admin.close();*/
    }
}