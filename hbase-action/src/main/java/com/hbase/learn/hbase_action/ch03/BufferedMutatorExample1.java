package com.hbase.learn.hbase_action.ch03;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutator.ExceptionListener;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.hbase.learn.common.HBaseHelper;

public class BufferedMutatorExample1 {
	private static final Log LOG = LogFactory.getLog(BufferedMutatorExample1.class);

	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		HBaseHelper helper = HBaseHelper.getHelper(conf);
		Connection conn = helper.getConnection();
		if (helper.existsTable("test_table")) {
			helper.dropTable("test_table");
		}
		helper.createTable("test_table", "colfams");
		TableName tableName = TableName.valueOf("test_table");
		Table table = conn.getTable(tableName);

		BufferedMutator.ExceptionListener listener = new ExceptionListener() {
			@Override
			public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator)
					throws RetriesExhaustedWithDetailsException {
				// TODO Auto-generated method stub
				for (int i = 0; i < e.getNumExceptions(); i++) {
					LOG.info("Exception " + e.getRow(i));
				}

			}
		};

		BufferedMutatorParams params = new BufferedMutatorParams(tableName).listener(listener);
		params.writeBufferSize(123123L);
		BufferedMutator mutator = conn.getBufferedMutator(params);

		Put put1 = new Put(Bytes.toBytes("row1"));
		put1.addColumn(Bytes.toBytes("colfams"), Bytes.toBytes("id"), Bytes.toBytes("1"));
		put1.addColumn(Bytes.toBytes("colfams"), Bytes.toBytes("name"), Bytes.toBytes("zhangsan1"));
		Put put2 = new Put(Bytes.toBytes("row2"));
		put2.addColumn(Bytes.toBytes("colfams"), Bytes.toBytes("id"), Bytes.toBytes("2"));
		put2.addColumn(Bytes.toBytes("colfams"), Bytes.toBytes("name"), Bytes.toBytes("zhangsan2"));

		mutator.mutate(put1);
		mutator.mutate(put2);

		mutator.close();
		table.close();
		helper.dump("test_table", new String[] { "row1", "row2" }, null, null);
		conn.close();
		helper.close();

	}

}
