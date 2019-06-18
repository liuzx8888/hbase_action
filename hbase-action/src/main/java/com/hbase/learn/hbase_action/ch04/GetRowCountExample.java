package com.hbase.learn.hbase_action.ch04;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.hbase.learn.hbase_action.ch04.getRowCountProtos.RowCountService;
import com.hbase.learn.hbase_action.ch04.getRowCountProtos.getRowCountRequest;
import com.hbase.learn.hbase_action.ch04.getRowCountProtos.getRowCountResponse;


public class GetRowCountExample extends RowCountService implements Coprocessor, CoprocessorService {

	 public  Log  LOG = LogFactory.getLog(GetRowCountExample.class);
			   
	
	private RegionCoprocessorEnvironment re;
	// 这两个类成员是后续代码用来操作 ZooKeeper 的，在 start() 中进行初始化
	private String zNodePath = "/hbase/ibmdeveloperworks/demo";
	private ZooKeeperWatcher zkw = null;

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
			RegionServerServices rss = re.getRegionServerServices();
			// 获取 ZooKeeper 对象，这个 ZooKeeper 就是本 HBase 实例所连接的 ZooKeeper
			zkw = rss.getZooKeeper();
			// 用 region name 作为 znode 的节点名后缀
			zNodePath = zNodePath + re.getRegionInfo().getRegionNameAsString();
		} else {
			throw new CoprocessorException("Must be loaded on a table region!");
		}

	}

	@Override
	public void stop(CoprocessorEnvironment env) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void getRowCount(RpcController controller, getRowCountRequest request,
			RpcCallback<getRowCountResponse> done) {
		// TODO Auto-generated method stub
		
		boolean reCount = request.getReCount();
		long rowcount = 0;
		if (reCount) {
			Scan scan = new Scan();
			scan.setMaxVersions(1);
			InternalScanner scanner = null;
			try {
				scanner = re.getRegion().getScanner(scan);
				List<Cell> results = new ArrayList<Cell>();
				boolean hasMore = false;
				long count = 0;
				while (scanner.next(results))
					do {
						hasMore = scanner.next(results);
						count++;
					} while (hasMore);
				rowcount=count;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} else {
			try {
				byte[] data = ZKUtil.getData(zkw, zNodePath);
				 rowcount = Bytes.toLong(data);
			} catch (Exception e) {
				LOG.info("Exception during getData");
			}
		}

		
		getRowCountProtos.getRowCountResponse response = null;
		response=getRowCountProtos.getRowCountResponse.newBuilder().setRowCount(rowcount).build();
		 //将 rowcount 设置为 CountResponse 消息的 rowCount
		 done.run(response); //Protobuf 的返回
		
	}

}
