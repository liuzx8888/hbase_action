package com.hbase.learn.hbase_action.ch03;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

import com.hbase.learn.hbase_action.common.HBaseHelper;

public class CheckAndMutateExample {
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		HBaseHelper helper = HBaseHelper.getHelper(conf);
		Connection conn = helper.getConnection();
		if (helper.existsTable("test_table")) {
			helper.dropTable("test_table");
		}
		helper.createTable("test_table", "fam-1", "fam-2");

		Table table = conn.getTable(TableName.valueOf("test_table"));

		helper.put("test_table", new String[] { "row-1", "row-2" }, new String[] { "fam-1", "fam-2" },
				new String[] { "col-1", "col-2", "col-3" }, new long[] { 1L, 2L, 3L },
				new String[] { "val-1", "val-2", "val-3" });
		System.out.println("init....");
		helper.dump("test_table");

		Put put = new Put(Bytes.toBytes("row-1"));
		put.addColumn(Bytes.toBytes("fam-1"), Bytes.toBytes("col-1"), 4, Bytes.toBytes("val99"));
		put.addColumn(Bytes.toBytes("fam-1"), Bytes.toBytes("col-4"), 4, Bytes.toBytes("val100"));

		Delete delete = new Delete(Bytes.toBytes("row-1"));
		delete.addColumn(Bytes.toBytes("fam-1"), Bytes.toBytes("col-2"));

		RowMutations mutations = new RowMutations(Bytes.toBytes("row-1"));
		mutations.add(put);
		mutations.add(delete);
		
//		table.mutateRow(mutations);
		
		Boolean rs= table.checkAndMutate(Bytes.toBytes("row-1"), Bytes.toBytes("fam-1"), Bytes.toBytes("col-1"), CompareOp.LESS, Bytes.toBytes("val-1"), mutations);
		System.out.println(" result1:" +  rs);
		
		System.out.println("after result1:");
		helper.dump("test_table");
		
		Put put1 = new Put(Bytes.toBytes("row-1"));
		put1.addColumn( Bytes.toBytes("fam-1"), Bytes.toBytes("col-1"),4, Bytes.toBytes("val-2"));	
		table.put(put1);
		
		System.out.println("after put1:");
		helper.dump("test_table");	
		
		
		Boolean rs1= table.checkAndMutate(Bytes.toBytes("row-1"), Bytes.toBytes("fam-1"), Bytes.toBytes("col-1"), CompareOp.LESS, Bytes.toBytes("val-1"), mutations);
		System.out.println(" result2:" +  rs1 );		
		
		System.out.println("after result2:");
		helper.dump("test_table");	
		

		conn.close();
		helper.close();

	}

}
