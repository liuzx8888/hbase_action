package com.hbase.learn.hbase_action.ch04;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;

import com.hbase.learn.common.HBaseHelper;
import com.hbase.learn.hbase_action.ch04.RowSumProtos.RowSumService;

public class CustomEndPointClient {
	public static void main(String[] args) throws Throwable {
		Configuration conf = HBaseConfiguration.create();
		HBaseHelper helper = HBaseHelper.getHelper(conf);
		Connection connection = ConnectionFactory.createConnection(conf);
		Table table= connection.getTable(TableName.valueOf("testtable_ep"));
		RowSumProtos.RowSumRequest request = RowSumProtos.RowSumRequest.newBuilder()
				.setStartKey("row-0")
				.setEndKey("row-6")
				.setFamily("colfam1")
				.setQuailty("c1")
				.build();
		

		Map<byte[], Long> rs = table.coprocessorService(RowSumService.class, null, null, 
				new Batch.Call<RowSumProtos.RowSumService, Long>() {
					public Long call(RowSumService instance) throws IOException {
						ServerRpcController controller = new ServerRpcController();
						BlockingRpcCallback<com.hbase.learn.hbase_action.ch04.RowSumProtos.RowSumResponse> RpcCallback = new BlockingRpcCallback<com.hbase.learn.hbase_action.ch04.RowSumProtos.RowSumResponse>();
						 instance.getRowSum(controller, request,RpcCallback );
						 RowSumProtos.RowSumResponse response= RpcCallback.get();
						 return response.getRowSum();
					}
				}
				);
		for (Entry<byte[], Long> entry : rs.entrySet()) {
			System.err.println(entry.getValue());
		}

		
	}

}
