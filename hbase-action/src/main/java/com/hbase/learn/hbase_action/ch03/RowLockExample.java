package com.hbase.learn.hbase_action.ch03;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class RowLockExample implements Runnable {
	HTable table = null;

	public void run() {
		// TODO Auto-generated method stub
		Configuration conf = HBaseConfiguration.create();
		try {
			table = new HTable(conf, "test_table");
			Put put = new Put(Bytes.toBytes("row2"));
			put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("c6"), Bytes.toBytes("val8"));
			put.add(Bytes.toBytes("colfam1"), Bytes.toBytes("c7"), Bytes.toBytes("val9"));
			table.put(put);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {

	}

}
