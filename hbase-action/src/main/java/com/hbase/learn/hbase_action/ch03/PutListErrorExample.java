package com.hbase.learn.hbase_action.ch03;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.hbase.learn.common.HBaseHelper;

public class PutListErrorExample {
	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		HBaseHelper help = HBaseHelper.getHelper(conf);
		if (help.existsTable("test_table")) {
			help.dropTable("test_table");
		}
		help.createTable("test_table", "colfam1");

		Connection connection = help.getConnection();
		Table table = connection.getTable(TableName.valueOf("test_table"));
		List<Put> puts = new ArrayList<Put>();

		Put put1 = new Put(Bytes.toBytes("row1"));
		put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("name"), Bytes.toBytes("zhangsan1"));

		Put put2 = new Put(Bytes.toBytes("row2"));
		put2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("name"), Bytes.toBytes("zhangsan2"));

		Put put3 = new Put(Bytes.toBytes("row3"));
		put3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("name"), Bytes.toBytes("zhangsan3"));

		/*
		 * 插入不存在的 列簇
		 */

		Put put4 = new Put(Bytes.toBytes("row4"));
		put4.addColumn(Bytes.toBytes("colfam1_1"), Bytes.toBytes("name"), Bytes.toBytes("zhangsan4"));
//
//		Put put5 = new Put(Bytes.toBytes("row4"));
//		puts.add(put5);

		puts.add(put1);
		puts.add(put2);
		puts.add(put3);
		puts.add(put4);
	//	puts.add(put5);

		try {
			table.put(puts);
		}
		/*
		 * 放开 put5
		 */
		// catch (Exception e) {
		// // TODO: handle exception
		// System.err.println(e);
		// }
		catch (RetriesExhaustedWithDetailsException e) {
			int numErrors = e.getNumExceptions(); // co PutListErrorExample3-2-Error Handle failed operations.
			System.out.println("Number of exceptions: " + numErrors);
			for (int n = 0; n < numErrors; n++) {
				System.out.println("Cause[" + n + "]: " + e.getCause(n));
				System.out.println("Hostname[" + n + "]: " + e.getHostnamePort(n));
				System.out.println("Row[" + n + "]: " + e.getRow(n)); // co PutListErrorExample3-3-ErrorPut Gain access
																		// to the failed operation.
			}
			System.out.println("Cluster issues: " + e.mayHaveClusterIssues());
			System.out.println("Description: " + e.getExhaustiveDescription());
		}

		table.close();
		connection.close();
		help.close();

	}

}
