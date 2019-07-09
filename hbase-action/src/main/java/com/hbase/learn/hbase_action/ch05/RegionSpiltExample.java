package com.hbase.learn.hbase_action.ch05;

import java.io.IOException;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

public class RegionSpiltExample {

	public byte[][] rows(int Spiltregions) throws InterruptedException {
		byte[][] splitKeys = new byte[Spiltregions][];
		TreeSet<byte[]> rows_rang = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);

		for (int i = 0; i < Spiltregions; i++) {
			String currenttime_Str = new String(Bytes.toBytes(System.currentTimeMillis()));
			long region_row = (System.currentTimeMillis()+i) % (Spiltregions);
			//System.out.println(region_row);
			rows_rang = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
			byte[] a = Bytes.add(
					 Bytes.toBytes(MD5Hash.getMD5AsHex(Bytes.toBytes(region_row)).substring(0,8))
					,Bytes.toBytes(i)
							
					);
			//System.out.println(new String(a));
			rows_rang.add(a);
		}
		
		int index = 0;
		Iterator<byte[]> iterator = rows_rang.iterator();
		while (iterator.hasNext()) {
			byte[] tempRow = iterator.next();
			splitKeys[index] = tempRow;
			index++;
		}
		rows_rang.clear();
		rows_rang = null;
		return splitKeys;

	}
	
	

	
	

	public static void main(String[] args) throws IOException, Exception {
		RegionSpiltExample example = new RegionSpiltExample();
		byte[][] splitKeys = example.rows(9);
//		Configuration conf = HBaseConfiguration.create();
//		HBaseHelper helper = HBaseHelper.getHelper(conf);
//		Connection conn = helper.getConnection();
//		Admin admin =  conn.getAdmin();
//	
//		if (helper.existsTable("testtable")){
//			helper.dropTable("testtable");
//		}
//		
//		byte[][] splitKeys = example.rows(10);
//		HTableDescriptor htd= new HTableDescriptor("testtable");
//		admin.createTable(htd, splitKeys);
		
	}
}

// ae2219b
// e3e1301
// ae2219b
// db572e5
// 7dea362
// fa5ad9a
// e675cc2
// 596be2d
// 59cff54
// aaa0745
