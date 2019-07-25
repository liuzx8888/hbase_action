package com.hbase.learn.hbase_action.ch03;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.hbase.learn.hbase_action.common.HBaseHelper;

public class AppendExample {
	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		HBaseHelper helper = HBaseHelper.getHelper(conf);

		if (helper.existsTable("test_table")) {
			helper.dropTable("test_table");
		}
		helper.createTable("test_table", "colfam1");
		Connection connection = helper.getConnection();
		Table table = connection.getTable(TableName.valueOf("test_table"));
		
		Put put = new Put(Bytes.toBytes("row1"));
		put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("c6"), Bytes.toBytes("val6")); 
		
		Append append1 = new Append(Bytes.toBytes("row1"));
		append1.add(Bytes.toBytes("colfam1"), Bytes.toBytes("c6"), Bytes.toBytes("val66"));
		table.put(put);
		table.append(append1);
		
//		Scan scan = new Scan();
//		ResultScanner rss =   table.getScanner(Bytes.toBytes("colfam1"));
//		for( Result rs : rss) {
//			System.out.println(rs);
//		}
		Get get = new Get(Bytes.toBytes("row1"));
		Result rs = table.get(get);
		System.out.println( Bytes.toString(rs.getValue(Bytes.toBytes("colfam1"), Bytes.toBytes("c6"))));
		
//		for (Cell cell : rs.rawCells()) {
//			 System.out.println("Cell: " + cell +
//			          ", Value: " + Bytes.toString(cell.getValueArray(),
//			          cell.getValueOffset(), cell.getValueLength()));
//		}
//		
		
		helper.dump(table.getName());
		
		table.close();
		connection.close();
		helper.close();
		

	}

}
