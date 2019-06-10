package com.hbase.learn.hbase_action.ch03;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.hbase.learn.common.HBaseHelper;

public class PutWriteBufferExample2 {
	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		HBaseHelper helper = HBaseHelper.getHelper(conf);
		Connection connection = helper.getConnection();

		Table table = connection.getTable(TableName.valueOf("test_table"));
		BufferedMutator mutator = connection.getBufferedMutator(table.getName());
		List<Mutation> mutations = new ArrayList<Mutation>();

		if (helper.existsTable(TableName.valueOf("test_table"))) {
			helper.dropTable(TableName.valueOf("test_table"));
		}
		helper.createTable("test_table", "colfam1");

		Put put1 = new Put(Bytes.toBytes("row1"));
		put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("id"), Bytes.toBytes("1"));
		put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("name"), Bytes.toBytes("test"));
		put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("sex"), Bytes.toBytes("Female"));
		put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("Job"), Bytes.toBytes("Worker"));
		mutations.add(put1);

		Put put2 = new Put(Bytes.toBytes("row2"));
		put2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("id"), Bytes.toBytes("2"));
		put2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("name"), Bytes.toBytes("test1"));
		put2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("sex"), Bytes.toBytes("Female"));
		put2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("Job"), Bytes.toBytes("Worker"));
		mutations.add(put2);

		Put put3 = new Put(Bytes.toBytes("row3"));
		put3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("id"), Bytes.toBytes("3"));
		put3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("name"), Bytes.toBytes("test2"));
		put3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("sex"), Bytes.toBytes("Female"));
		put3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("Job"), Bytes.toBytes("Worker"));
		mutations.add(put3);

		mutator.mutate(mutations);
		mutator.flush();

		Scan scan = new Scan();
		ResultScanner rss = table.getScanner(Bytes.toBytes("colfam1"));
		for (Result rs : rss) {
			System.out.println(rs);
		}

		mutator.close();
		table.close();
		connection.close();
		helper.close();

	}

}
