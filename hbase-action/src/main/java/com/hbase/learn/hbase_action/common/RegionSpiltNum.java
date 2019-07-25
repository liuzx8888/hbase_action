package com.hbase.learn.hbase_action.common;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

public class RegionSpiltNum {
	/*
	 * 获取当前有多少个服务器 计算集群region数量的公式：((RS Xmx) *
	 * hbase.regionserver.global.memstore.size) / (hbase.hregion.memstore.flush.size
	 * * (# column families))
	 */

	private static int numberOfRregion = 1;
	private  int memstoreSize =8;
	private static float serversNum = 4;
	private static int deadserversNum = 0;
	private static float memstoreUppLimit = (float) 0.4;
	private static float memstoreFlush = 134217728;
	
	
	public RegionSpiltNum(int memstoreSize) {
		this.memstoreSize = memstoreSize;
	}

	

	public int regionNum() throws IOException {
		Configuration conf = HBaseConfiguration.create();

		HBaseHelper helper = HBaseHelper.getHelper(conf);
		Connection conn = helper.getConnection();
		Admin admin = conn.getAdmin();
		serversNum = admin.getClusterStatus().getServersSize();
		deadserversNum = admin.getClusterStatus().getDeadServers();
		memstoreUppLimit = Float.valueOf(conf.get("hbase.regionserver.global.memstore.upperLimit", "0.4"));
		memstoreFlush = Float.valueOf(conf.get("hbase.hregion.memstore.flush.size", "134217728")) / 1024 / 1024;
			
//		System.out.println(
//				  "  serversNum :" +serversNum                        
//				+ "  deadserversNum :" +deadserversNum                
//				+ "  memstoreUppLimit :" +memstoreUppLimit            	
//				+ "  memstoreFlush :" +memstoreFlush	                	
//				+ "  memstoreSize :" +memstoreSize	                			
//				);
		 
		int RregionNum =  (int) (
				(memstoreSize * 1024 * memstoreUppLimit/ memstoreFlush)
				* 
				(serversNum - deadserversNum)
				);

		return RregionNum;
	}
	
	public static void main(String[] args) {
		RegionSpiltNum num = new RegionSpiltNum(8);
		int number = 0;
		try {
			number = num.regionNum();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(number);
		
	}

}
