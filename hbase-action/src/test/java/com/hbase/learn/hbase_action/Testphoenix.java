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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

import com.hbase.learn.hbase_action.ch05.RegionSpiltNum;

public class Testphoenix {
	public static void main(String[] args) throws SQLException, InterruptedException, IOException {

		Statement stmt = null;
		ResultSet rset = null;

		Connection con = DriverManager.getConnection("jdbc:phoenix:hadoop1:2181");
		stmt = con.createStatement();
		stmt.executeUpdate("DROP TABLE testtable_htd");
		int regionNum =new RegionSpiltNum(8).regionNum();
		stmt.executeUpdate("create table IF NOT EXISTS testtable_htd (mykey varchar not null primary key, id varchar,name varchar,sex varchar) SALT_BUCKETS ="+regionNum+"");

		char[] A_z = { 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S',
				'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n',
				'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z' };
		Random r = new Random();

		for (int i = 0; i < 10; i++) {
			String rowkey = MD5Hash.getMD5AsHex(Bytes.toBytes(A_z[r.nextInt(A_z.length)])).substring(0, 8)+"-"+ String.valueOf(i);
			System.out.println(rowkey);
			stmt.executeUpdate(
//					"UPSERT INTO TEST (mykey,mycolumn) VALUES ('" + rowkey + "','" + String.valueOf(i)+"')");
					"UPSERT INTO 'testtable_htd' (ROW,id,name,sex,timestamp) "
					+ "VALUES "
					+ "('" + rowkey + "','" + String.valueOf(i)+"',"+String.valueOf(i)+", 'TEST_"+String.valueOf(i)+"',)"
					
					);
		}
	
		con.commit();
		
		PreparedStatement statement = con.prepareStatement("select * from test");
		rset = statement.executeQuery();
		while (rset.next()) {
			System.out.println(rset.getString("mycolumn"));
		}
		statement.close();
		con.close();
	}

}
