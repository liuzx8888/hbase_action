package com.hbase.learn.hbase_action.ch03;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;


public class DelExample {
	public static void main(String[] args) throws IOException {

		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, "test_table");

		Delete del1 = new Delete(Bytes.toBytes("row1"));

		/*
		 * 单行删除
		 */
		del1.setTimestamp(0);

		del1.deleteColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("c2"));
		table.delete(del1);
		
//		del1.deleteFamily(Bytes.toBytes("colfam1"));
		
		
		/*
		 * 多行删除
		 */		
		
		Delete del2 = new Delete(Bytes.toBytes("row1"));	
		del2.deleteColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("c3"));
		
		
		List<Delete> dels  = new ArrayList<Delete>();
		dels.add(del1);
		dels.add(del2);
		table.delete(dels);
		table.close();
		

	}

}
