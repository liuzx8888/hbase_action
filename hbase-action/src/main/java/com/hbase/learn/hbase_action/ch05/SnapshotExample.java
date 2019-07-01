package com.hbase.learn.hbase_action.ch05;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;

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
		admin.snapshot("snapshot1", TableName.valueOf("testtable_ep"));

		Delete delete = new Delete(Bytes.toBytes("row8"));
		delete.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("c2")); // co SnapshotExample-2-Delete Remove one
																			// column and do two more snapshots, one
																			// without first flushing, then another with
																			// a preceding flush.
		table.delete(delete);
		admin.snapshot("snapshot2", tableName, HBaseProtos.SnapshotDescription.Type.SKIPFLUSH);
		admin.snapshot("snapshot3", tableName, HBaseProtos.SnapshotDescription.Type.FLUSH);

		table.close();
		conn.close();
		helper.close();

	}

}
