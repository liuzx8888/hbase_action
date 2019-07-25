package com.hbase.learn.hbase_action.ch03;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.hbase.learn.hbase_action.common.HBaseHelper;

public class PutExample {
	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		HBaseHelper helper = HBaseHelper.getHelper(conf);

		if (helper.existsTable("test_table")) {
			helper.dropTable("test_table");
		}
		helper.createTable("test_table", "colfam1");

		Connection connection = helper.getConnection();
		// Table table = connection.getTable(TableName.valueOf("test_table"));
		HTable table = new HTable(conf, "test_table");
		Put put = new Put(Bytes.toBytes("row1"));
		put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("c6"), Bytes.toBytes("val6"));

		// 启用客户端写缓存
		// table.setAutoFlush(true);
		// table.put(put);
		// table.flushCommits();

		// 原子性操作
		Boolean rs = table.checkAndPut(Bytes.toBytes("row1"), Bytes.toBytes("colfam1"), Bytes.toBytes("c6"), null, put);
		System.out.println("rs:" + rs);

		Boolean rs1 = table.checkAndPut(Bytes.toBytes("row1"), Bytes.toBytes("colfam1"), Bytes.toBytes("c6"), null,put);
		System.out.println("rs1:" + rs1);

		Put put1 = new Put(Bytes.toBytes("row1"));
		put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("c7"), Bytes.toBytes("val7"));

		Boolean rs2 = table.checkAndPut(Bytes.toBytes("row1"), Bytes.toBytes("colfam1"), Bytes.toBytes("c7"),
				Bytes.toBytes("val7"), put1);
		System.out.println("rs2:" + rs2);	
		
		Boolean rs3 = table.checkAndPut(Bytes.toBytes("row1"), Bytes.toBytes("colfam1"), Bytes.toBytes("c7"), null,
				put1);
		System.out.println("rs3:" + rs3);
		

		
		Put put2 = new Put(Bytes.toBytes("row1"));
		put2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("c6"), Bytes.toBytes("val9"));
		Boolean rs4 = table.checkAndPut(Bytes.toBytes("row1"), Bytes.toBytes("colfam1"), Bytes.toBytes("c6"), Bytes.toBytes("val6"),
				put2);
		System.out.println("rs4:" + rs4);
		
		Boolean rs5 = table.checkAndPut(Bytes.toBytes("row1"), Bytes.toBytes("colfam1"), Bytes.toBytes("c6"), null,
				put2);
		System.out.println("rs5:" + rs5);
		
		
		Put put3 = new Put(Bytes.toBytes("row2"));
		put3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("name"),  Bytes.toBytes("zhangsan1"));
		put3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("name"),  Bytes.toBytes("zhangsan2"));
		table.put(put3);

		
		table.close();
		connection.close();
		helper.close();
	}

}
