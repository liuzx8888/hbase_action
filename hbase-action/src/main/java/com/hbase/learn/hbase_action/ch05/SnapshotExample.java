package com.hbase.learn.hbase_action.ch05;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;

import com.hbase.learn.common.HBaseHelper;

public class SnapshotExample {
	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		HBaseHelper helper = HBaseHelper.getHelper(conf);
		Connection conn = helper.getConnection();
		TableName tableName = TableName.valueOf("testtable_ep");
		Table table = conn.getTable(tableName);
		Admin admin = conn.getAdmin();
		admin.deleteSnapshots("snapshot.*");
	    admin.snapshot("snapshot1", tableName); 
	    
		
	}

}
