package com.hbase.learn.hbase_action.ch04;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;

import com.hbase.learn.common.HBaseHelper;

public class HtablePoolExample {
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		HBaseHelper helper = HBaseHelper.getHelper(conf);
		Connection conn = helper.getConnection();


		HTablePool pool = new HTablePool(conf, Integer.MAX_VALUE);
		HTableInterface[] tables = new HTableInterface[10];
		tables [0] = pool.getTable("testtable");
		pool.putTable(tables [0]);
		System.out.println( Bytes.toString( tables [0].getTableName() ));
		pool.closeTablePool("testtable");

	}

}
