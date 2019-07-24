package com.hbase.learn.hbase_action;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Random;

import org.antlr.runtime.ANTLRReaderStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

import com.hbase.learn.common.HBaseHelper;
import com.hbase.learn.hbase_action.ch05.RegionSpiltNum;
public class Testphoenix {
	
	
	public static void main(String[] args) throws SQLException, InterruptedException, IOException {
//        Configuration conf = HBaseConfiguration.create();
//        HBaseHelper helper = HBaseHelper.getHelper(conf);
//        org.apache.hadoop.hbase.client.Connection  conn =  helper.getConnection();
//        Admin admin = conn.getAdmin();
//        Scan scan = new Scan().setLimit(1);
//		scan.addFamily(Bytes.toBytes("colfam1"))
//		.setReversed(false);
//        Table table  = conn.getTable(TableName.valueOf("testtable_htd"));
//        ResultScanner rs = table.getScanner(scan);
//        System.out.println("rs"+rs.next().getRow());
		 
		Statement stmt = null;
		ResultSet rset = null;

		Connection con = DriverManager.getConnection("jdbc:phoenix:hadoop1:2181");
		stmt = con.createStatement();
		//stmt.executeUpdate("DROP TABLE TEST");
		int regionNum =new RegionSpiltNum(8).regionNum();
		//stmt.executeUpdate("create table IF NOT EXISTS TEST (mykey varchar not null primary key, mycolumn varchar) SALT_BUCKETS ="+regionNum+"");

		char[] A_z = { 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S',
				'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n',
				'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z' };
		Random r = new Random();

//		for (int i = 0; i < 10; i++) {
//			String rowkey = MD5Hash.getMD5AsHex(Bytes.toBytes(A_z[r.nextInt(A_z.length)])).substring(0, 8)+"-"+ String.valueOf(i);
//			
//			System.out.println(rowkey);
//			int calchash = hash(Bytes.toBytes(rowkey), 1, rowkey.length()-1);
//			byte region_Loaction =(byte) Math.abs(calchash%regionNum);
//			
//			
//			System.out.println(region_Loaction);
//			
//			stmt.executeUpdate(
//					"UPSERT INTO TEST (mykey,mycolumn) VALUES ('" + rowkey + "','" + String.valueOf(i)+"')");
//		}
//		con.commit();
		
		//String tablenameS ="testtable_htd";
		PreparedStatement statement = con.prepareStatement("select * from  \"testtable_htd\" limit 1000");
		rset = statement.executeQuery();
		while (rset.next()) {
			byte[] newrowkey = new byte[rset.getString("ROW").length()-1];
			byte[] oldrowkey = Bytes.toBytes(rset.getString("ROW")) ;
			System.arraycopy(oldrowkey, 1, newrowkey, 0, oldrowkey.length-1);
			System.out.println(new String(newrowkey));
		}
		statement.close();
		con.close();
	}

}
