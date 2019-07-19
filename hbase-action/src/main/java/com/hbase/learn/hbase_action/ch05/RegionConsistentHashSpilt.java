package com.hbase.learn.hbase_action.ch05;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.hbase.learn.common.HBaseHelper;

public class RegionConsistentHashSpilt {

	static TreeSet<String> region_ranger = new TreeSet<String>(new MyCompare());

	/*
	 * 获取当前有多少个服务器 计算集群region数量的公式：((RS Xmx) *
	 * hbase.regionserver.global.memstore.size) / (hbase.hregion.memstore.flush.size
	 * * (# column families))
	 */

	private static int numberOfRregion = 1;
	private static float serversNum = 1;	
	private static int deadserversNum =0;
	private static float memstoreUppLimit = (float) 0.4 ;
	private static float memstoreFlush =134217728;
	
	
	public  RegionConsistentHashSpilt(int numberOfRregion) {
		this.numberOfRregion = numberOfRregion;
	}

	
	public static byte[][] splitRegionKey() {
		
	
		for (int i = 1; i < numberOfRregion; i++) {
			region_ranger.add("region_" + String.valueOf(i));
		}
		
		byte[][] byteRegionKey = new byte[numberOfRregion-1][];

		int i = 0;
		Iterator<String> iterator = region_ranger.iterator();
		while (iterator.hasNext()) {
			byte[] temp_key = Bytes.toBytes(iterator.next());
			byteRegionKey[i] = temp_key;
			i++;
		}

		region_ranger.clear();
		return byteRegionKey;
	}

	public static String getRegion(Object key) {
		ConsistentHash<String> consistentHash = new ConsistentHash<String>(3, region_ranger);
		String region_hash = consistentHash.get(key);
		return region_hash;
	}

}

class MyCompare implements Comparator {
	public int compare(Object o1, Object o2) {
		int num;

		Integer i1 = Integer.parseInt((o1).toString().replaceFirst("region_", ""));
		Integer i2 = Integer.parseInt((o2).toString().replaceFirst("region_", ""));

		num = i1.compareTo(i2);
		if (num == 0)
			return 0;
		return 1;
	}
}
