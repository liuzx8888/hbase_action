package com.hbase.learn.hbase_action.ch04;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import com.hbase.learn.common.HBaseHelper;

public class TestCoprocess {
public static void main(String[] args) throws Exception {
	Configuration conf = HBaseConfiguration.create();
	HBaseHelper helper = HBaseHelper.getHelper(conf);
	helper.dropTable("testtable");
	helper.createTable("testtable", "colfam1", "colfam2");
	System.out.println("Adding rows to table...");
	helper.fillTable("testtable", 1, 10, 10, "colfam1", "colfam2");
	Connection connection = ConnectionFactory.createConnection(conf);
	Table table = connection.getTable(TableName.valueOf("testtable"));

	

}
}
