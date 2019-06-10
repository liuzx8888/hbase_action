package com.hbase.learn.hbase_action.ch03;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.util.Bytes;

import com.hbase.learn.common.HBaseHelper;

public class BatchExample {
	public static void main(String[] args) throws IOException, InterruptedException {
		Configuration conf = HBaseConfiguration.create();
		HBaseHelper helper = HBaseHelper.getHelper(conf);
		Connection connection = helper.getConnection();
		Table table = connection.getTable(TableName.valueOf("test_table"));

		if (helper.existsTable("test_table")) {
			helper.dropTable("test_table");
		}
		helper.createTable("test_table", "colfam1");
		
		

		Put put = new Put(Bytes.toBytes("row1"));
		put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("c6"), Bytes.toBytes("val6"));
		put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("c7"), Bytes.toBytes("val7"));

		Get get1 = new Get(Bytes.toBytes("row1"));
		get1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("c6"));

		Get get2 = new Get(Bytes.toBytes("row1"));
		get2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("c7"));

		Delete del1 = new Delete(Bytes.toBytes("row2"));
		del1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("c5"));

		List<Row> batch = new ArrayList<Row>();
		batch.add(get1);
		batch.add(get2);
		batch.add(put);
		batch.add(del1);
		Object[] results = new Object[batch.size()]; 
		table.batch(batch,results);
		  try {
		      table.batch(batch, results);
		    } catch (Exception e) {
		      System.err.println("Error: " + e); // co BatchExample-7-Print Print error that was caught.
		    }

		    for (int i = 0; i < results.length; i++) {
		      System.out.println("Result[" + i + "]: type = " + // co BatchExample-8-Dump Print all results and class types.
		        results[i].getClass().getSimpleName() + "; " + results[i]);
		    }
		
		
		table.close();
	    System.out.println("After batch call...");
	    helper.dump("test_table", new String[]{"row1", "row2"}, null, null);
	    connection.close();
	    helper.close();
	}

}
