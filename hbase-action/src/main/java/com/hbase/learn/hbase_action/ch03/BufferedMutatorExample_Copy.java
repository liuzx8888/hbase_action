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
import org.apache.hadoop.hbase.util.Bytes;

import com.hbase.learn.common.HBaseHelper;

public class BufferedMutatorExample_Copy {
	private static final Log Log = LogFactory.getLog(BufferedMutatorExample_Copy.class);
	private static final int POOL_SIZE = 10;
	private static final int TASK_COUNT = 100;
	private static final TableName TABLE = TableName.valueOf("testtable");
	private static final byte[] FAMILY = Bytes.toBytes("colfam1");

	public static void main(String[] args) throws Exception {
		Configuration configuration = HBaseConfiguration.create();
		// ^^ BufferedMutatorExample
		HBaseHelper helper = HBaseHelper.getHelper(configuration);
		helper.dropTable("testtable");
		helper.createTable("testtable", "colfam1");

		BufferedMutator.ExceptionListener listener = new ExceptionListener() {
			@Override
			public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator)
					throws RetriesExhaustedWithDetailsException {
				// TODO Auto-generated method stub
				for (int i = 0; i < e.getNumExceptions(); i++) {
					Log.info("Failed to send put: " + e.getRow(i));
				}

			}
		};

		BufferedMutatorParams params = new BufferedMutatorParams(TABLE).listener(listener);

		try (
				Connection conn = helper.getConnection(); 
				BufferedMutator mutator = conn.getBufferedMutator(params);
			) {
			
			ExecutorService workerPool = Executors.newFixedThreadPool(POOL_SIZE);
			List<Future<Void>> futures = new ArrayList<>(TASK_COUNT);

			for (int i = 0; i < TASK_COUNT; i++) {
	
				futures.add(workerPool.submit(new Callable<Void>() {
					@Override
					public Void call() throws Exception {
						// TODO Auto-generated method stub
						Put p = new Put(Bytes.toBytes("row1"));
					
						p.addColumn(FAMILY, Bytes.toBytes("c1"),Bytes.toBytes("val1"));
						p.addColumn(FAMILY, Bytes.toBytes("c2"),Bytes.toBytes("val2"));	
						mutator.mutate(p);
						return null;
					}
				}));
			}
			for (Future<Void> f : futures) {
				f.get(5, TimeUnit.MINUTES);
			}
			workerPool.shutdown();
		} catch (IOException e) {
			Log.info("Exception while creating or freeing resources", e);
		}

	}
}
