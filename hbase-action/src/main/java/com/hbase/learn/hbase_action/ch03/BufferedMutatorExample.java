package com.hbase.learn.hbase_action.ch03;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.BufferedMutator.ExceptionListener;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.hbase.learn.common.HBaseHelper;
import com.hbase.learn.hbase_action.ch05.RegionConsistentHash;

public class BufferedMutatorExample {

	private static final Log LOG = LogFactory.getLog(BufferedMutatorExample.class);

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		HBaseHelper helper = HBaseHelper.getHelper(conf);
		// helper.dropTable("test_table");
		// helper.createTable("test_table", "col1");
		Connection conn = helper.getConnection();
		Table table = conn.getTable(TableName.valueOf("testtable_htd"));

		BufferedMutator.ExceptionListener listener = new ExceptionListener() {
			@Override
			public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator)
					throws RetriesExhaustedWithDetailsException {
				// TODO Auto-generated method stub
				for (int i = 0; i < e.getNumExceptions(); i++) {
					LOG.info("Failed to sent put " + e.getRow(i) + ".");
				}

			}
		};
		BufferedMutatorParams params = new BufferedMutatorParams(table.getName()).listener(listener);
		params.writeBufferSize(123123L);

		try {

			BufferedMutator mutator = conn.getBufferedMutator(params);

			for (int i = 1; i < 100000000; i++) {
				// Put put1 = new Put(Bytes.toBytes("row"+ String.valueOf(i)));

				Put put1 = new Put(Bytes.toBytes(
						RegionConsistentHash.getRegion("row" + String.valueOf(i)) +"-"+ String.valueOf(i)));

				put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("id"), Bytes.toBytes(String.valueOf(i)));
				put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("name"),
						Bytes.toBytes("test" + String.valueOf(i)));
				put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("sex"), Bytes.toBytes("male"));

				// Put put2 = new Put(Bytes.toBytes("row"+ String.valueOf(i)));
				// put1.addColumn(Bytes.toBytes("col1"), Bytes.toBytes("id"), Bytes.toBytes(i));
				// put1.addColumn(Bytes.toBytes("col1"), Bytes.toBytes("name"),
				// Bytes.toBytes("test"+ String.valueOf(i)));
				// put1.addColumn(Bytes.toBytes("col1"), Bytes.toBytes("sex"),
				// Bytes.toBytes("male"));
				// mutator.mutate(put1);
				// mutator.mutate(put2);
				mutator.mutate(put1);
			}
			helper.dump("testtable_htd", new String[] { "row1", "row2" }, null, null);

			mutator.close();
			table.close();
			System.out.println("After batch call...");
			helper.dump("testtable_htd", new String[] { "row1", "row2" }, null, null);
			conn.close();
			helper.close();

		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}

	}

}
