package com.hbase.learn.hbase_action.ch05;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class RegionConsistentHash {

	static TreeSet<String> region_ranger = new TreeSet<String>(new MyCompare());

	/*
	 * 默认每张表建表的时候 默认有 10个region 命名规则---默认 region_i i--> 1到10 使用Hash一致性算法
	 */
	private static int  numberOfRregion = 10;
	
	
	
	
	
	static {
		for (int i = 0; i < numberOfRregion; i++) {
			region_ranger.add("region_"+ String.valueOf(i));
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
		
		return byteRegionKey;
	}

	
	public static String getRegion(Object key) {
		ConsistentHash<String> consistentHash = new ConsistentHash<String>(numberOfRregion, region_ranger);
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
