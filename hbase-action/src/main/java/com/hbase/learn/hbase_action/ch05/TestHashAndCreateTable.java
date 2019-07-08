package com.hbase.learn.hbase_action.ch05;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

public class TestHashAndCreateTable {
	public static void main(String[] args) throws IOException, ZooKeeperConnectionException, IOException {
		HashChoreWoker worker = new HashChoreWoker(100, 2);
		byte[][] splitKeys = worker.calcSplitKeys();

		byte b[] = Bytes.toBytes("maizi");
		byte a[] = Bytes.toBytes("hello");

		// 多个字节，拼装成一个row key
		byte c[] = Bytes.add(a, b);
		System.out.println(Bytes.toString(c));

		// HBaseAdmin admin = new HBaseAdmin(HBaseConfiguration.create());
		// TableName tableName = TableName.valueOf("hash_split_table");
		//
		// if (admin.tableExists(tableName)) {
		// try {
		// admin.disableTable(tableName);
		// } catch (Exception e) {
		// }
		// admin.deleteTable(tableName);
		// }
		//
		// HTableDescriptor tableDesc = new HTableDescriptor(tableName);
		// HColumnDescriptor columnDesc = new HColumnDescriptor(Bytes.toBytes("info"));
		// columnDesc.setMaxVersions(1);
		// tableDesc.addFamily(columnDesc);
		//
		// admin.createTable(tableDesc ,splitKeys);
		//
		// admin.close();
	}
}
