package com.hbase.learn.hbase_action.ch04;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.hbase.learn.common.HBaseHelper;

public class IncrementSingleExample {
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		HBaseHelper helper = HBaseHelper.getHelper(conf);
		helper.dropTable("testtable");
		helper.createTable("testtable", "daily", "weekly", "monthly");
		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf("testtable"));

		long incr1 = table.incrementColumnValue(Bytes.toBytes("20110101"), Bytes.toBytes("daily"),
				Bytes.toBytes("hits"), 1);
		long incr2 = table.incrementColumnValue(Bytes.toBytes("20110101"), Bytes.toBytes("daily"),
				Bytes.toBytes("hits"), 1);
		long incr3 = table.incrementColumnValue(Bytes.toBytes("20110101"), Bytes.toBytes("daily"),
				Bytes.toBytes("hits"), 0);
		long incr4 = table.incrementColumnValue(Bytes.toBytes("20110101"), Bytes.toBytes("daily"),
				Bytes.toBytes("hits"), -1);

		long incr11 = table.incrementColumnValue(Bytes.toBytes("201101"), Bytes.toBytes("monthly"),
				Bytes.toBytes("hits"), 100);
		long incr22 = table.incrementColumnValue(Bytes.toBytes("201101"), Bytes.toBytes("monthly"),
				Bytes.toBytes("hits"), 100);
		long incr33 = table.incrementColumnValue(Bytes.toBytes("201101"), Bytes.toBytes("monthly"),
				Bytes.toBytes("hits"), 0);
		long incr44 = table.incrementColumnValue(Bytes.toBytes("201101"), Bytes.toBytes("monthly"),
				Bytes.toBytes("hits"), -57);

		System.out.println("cnt1: " + incr1 + ", cnt2: " + incr2 + ", current: " + incr3 + ", cnt4: " + incr4);

		System.out.println("cnt11: " + incr11 + ", cnt22: " + incr22 + ", current: " + incr33 + ", cnt44: " + incr44);

	}
}
