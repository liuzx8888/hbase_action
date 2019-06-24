package com.hbase.learn.hbase_action.ch04;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.hbase.learn.hbase_action.ch04.RowSumProtos.RowSumRequest;
import com.hbase.learn.hbase_action.ch04.RowSumProtos.RowSumResponse;
import com.hbase.learn.hbase_action.ch04.RowSumProtos.RowSumService;

public class CustomEndPointService extends RowSumService implements Coprocessor, CoprocessorService {
	public Log LOG = LogFactory.getLog(CustomEndPointService.class);
	private RegionCoprocessorEnvironment re;

	@Override
	public Service getService() {
		// TODO Auto-generated method stub
		return this;
	}

	@Override
	public void start(CoprocessorEnvironment env) throws IOException {
		// TODO Auto-generated method stub
		if (env instanceof RegionCoprocessorEnvironment) {
			this.re = (RegionCoprocessorEnvironment) env;
		}

	}

	@Override
	public void stop(CoprocessorEnvironment env) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void getRowSum(RpcController controller, RowSumRequest request, RpcCallback<RowSumResponse> done) {
		// TODO Auto-generated method stub

		String start_key = request.getStartKey();
		String end_key = request.getEndKey();
		String fam = request.getFamily();
		String qua = request.getQuailty();
		int sum = 0;

		Scan scan = new Scan();
		scan.setMaxVersions(1);
		scan.addColumn(Bytes.toBytes(fam), Bytes.toBytes(qua));
		Filter filter = new CustomFilterL(start_key, end_key);
		scan.setFilter(filter);
		try {
			RegionScanner scanner = re.getRegion().getScanner(scan);
			 List<Cell> results = new ArrayList<Cell>();

			while(scanner.next(results)) {
			  		for (Cell result : results) {
						sum = sum + Bytes.toInt(result.getValueArray(), result.getValueOffset(), result.getValueLength());
					}	
			}
			

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// getRowCountProtos.getRowCountResponse response = null;
		// response=getRowCountProtos.getRowCountResponse.newBuilder().setRowCount(rowcount).build();
		// //将 rowcount 设置为 CountResponse 消息的 rowCount
		// done.run(response); //Protobuf 的返回
		RowSumProtos.RowSumResponse response = null;
		response = RowSumProtos.RowSumResponse.newBuilder().setRowSum(sum).build();
		done.run(response);

	}

}
