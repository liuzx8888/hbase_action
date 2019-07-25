package com.hbase.learn.hbase_action.ch03;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.hbase.learn.hbase_action.common.HBaseHelper;

public class ScanExample {
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		HBaseHelper helper = HBaseHelper.getHelper(conf);
		Connection conn = helper.getConnection();

//		helper.dropTable("testtable");
//		helper.createTable("testtable", "colfam1", "colfam2");
//		System.out.println("Adding rows to table...");
//		// Tip: Remove comment below to enable padding, adjust start and stop
//		// row, as well as columns below to match. See scan #5 comments.
//		helper.fillTable("testtable", 1, 100, 2, /* 3, false, */ "colfam1", "colfam2");

		Table table = conn.getTable(TableName.valueOf("testtable_htd"));

		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("colfam1"))
		        .setLimit(1000)
//		        .setStartRow(Bytes.toBytes("row-10"))
//				.setStopRow(Bytes.toBytes("row-20"))
				.setReversed(false);

		ResultScanner result = table.getScanner(scan);
		for (Result rs : result) {
			byte[] newrowkey = new byte[ rs.getRow().length-1];
			byte[] oldrowkey = rs.getRow();
			System.arraycopy(oldrowkey, 1, newrowkey, 0, oldrowkey.length-1);
			System.out.println(new String(newrowkey));
		}

	}

}
