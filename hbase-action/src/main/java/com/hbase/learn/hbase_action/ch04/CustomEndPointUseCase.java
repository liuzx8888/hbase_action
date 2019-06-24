package com.hbase.learn.hbase_action.ch04;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.RpcCallback;
import com.hbase.learn.common.HBaseHelper;
import com.hbase.learn.hbase_action.ch04.getRowCountProtos.RowCountService;

public class CustomEndPointUseCase {
	public static void main(String[] args) throws Throwable {
		Configuration conf = HBaseConfiguration.create();
		HBaseHelper helper = HBaseHelper.getHelper(conf);
		Connection conn = helper.getConnection();

		Table table = conn.getTable(TableName.valueOf("testtable_ep"));

		getRowCountProtos.getRowCountRequest request = getRowCountProtos.getRowCountRequest.newBuilder()
				.setReCount(true).build();

		Map<byte[], Long> rs = table.coprocessorService(RowCountService.class, null, null,
				new Batch.Call<RowCountService, Long>() {

					@Override
					public Long call(RowCountService instance) throws IOException {
						ServerRpcController controller = new ServerRpcController();
						BlockingRpcCallback<com.hbase.learn.hbase_action.ch04.getRowCountProtos.getRowCountResponse> rpcCallback = new BlockingRpcCallback<com.hbase.learn.hbase_action.ch04.getRowCountProtos.getRowCountResponse>();
						
						instance.getRowCount(controller, request, rpcCallback);
						getRowCountProtos.getRowCountResponse response = rpcCallback.get();
				
						return response.getRowCount() ;

						// TODO Auto-generated method stub

					}
				});
		System.out.println(rs.values());

		
	}

}
