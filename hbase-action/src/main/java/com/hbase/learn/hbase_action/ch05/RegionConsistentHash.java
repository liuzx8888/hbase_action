package com.hbase.learn.hbase_action.ch05;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.hbase.learn.common.HBaseHelper;

public class RegionConsistentHash {

	static TreeSet<String> region_ranger = new TreeSet<String>(new MyCompare());

	/*
	 * 
	 * 获取当前有多少个服务器
	 * 计算集群region数量的公式：((RS Xmx) * hbase.regionserver.global.memstore.size) 
	 * / (hbase.hregion.memstore.flush.size * (# column families))
	 */
	
	private static int  numberOfRregion ;
	private static int  memstoreSize =8*1024 ;
	static {
		
		Configuration  conf =  HBaseConfiguration.create();
		try {
			HBaseHelper helper=  HBaseHelper.getHelper(conf);
			Connection conn =  helper.getConnection();
			Admin admin = conn.getAdmin();
			int serversNum =admin.getClusterStatus().getServersSize();
			int deadserversNum =admin.getClusterStatus().getDeadServers();
			float memstoreUppLimit=Float.valueOf(conf.get("hbase.regionserver.global.memstore.upperLimit","0.4"));
			float memstoreFlush=Float.valueOf(conf.get("hbase.hregion.memstore.flush.size","134217728"))/1024/1024;
			
			numberOfRregion = (int) ((int)(memstoreSize)*memstoreUppLimit/memstoreFlush)*(serversNum-deadserversNum);
			
			for (int i = 0; i < numberOfRregion; i++) {
				region_ranger.add("region_"+ String.valueOf(i));
			}
		
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	}
	
	
	public static byte[][] splitRegionKey (){
		byte[][] byteRegionKey = new byte[numberOfRregion][];
		
		int i=0;
		Iterator<String> iterator = region_ranger.iterator();
		while (iterator.hasNext()) {
			 byte[] temp_key = Bytes.toBytes( iterator.next()) ;
			 byteRegionKey[i]=temp_key;
			 i++; 
		}
		
		region_ranger.clear();
		return byteRegionKey;
	}

	
	public static String getRegion(Object key) {
		ConsistentHash<String> consistentHash = new ConsistentHash<String>(3, region_ranger);
		String region_hash =  consistentHash.get(key);
		return region_hash;
	}


	

}

class MyCompare implements Comparator{
    public int compare(Object o1,Object o2){
        int num;
        
        Integer i1=Integer.parseInt((o1).toString().replaceFirst("region_", ""));
        Integer i2=Integer.parseInt((o2).toString().replaceFirst("region_", ""));       
        
        num=i1.compareTo(i2);
        if(num==0)
            return 0;
        return 1;
    }
}
