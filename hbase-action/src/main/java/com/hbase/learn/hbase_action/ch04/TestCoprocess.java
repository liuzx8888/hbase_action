package com.hbase.learn.hbase_action.ch04;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;

import com.hbase.learn.hbase_action.common.HBaseHelper;

public class TestCoprocess {
public static void main(String[] args) throws Exception {
	Configuration conf = HBaseConfiguration.create();
	HBaseHelper helper = HBaseHelper.getHelper(conf);
	Connection connection = ConnectionFactory.createConnection(conf);
	
	helper.dropTable("testtable");
//	helper.dropTable("testtable_idx");	
//	helper.dropTable("testtable_idx_idx");	
	
	HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("testtable"));
	htd.addFamily(new HColumnDescriptor("colfam1"));
	htd.setValue("COPROCESSOR$1", "hdfs://hadoop1:8020/user/hbase/customCoprocessor/index.jar" + "|"
			+ "com.hbase.learn.hbase_action.ch04.RegionObserverExample2"+ "|" + Coprocessor.PRIORITY_USER);

	
	Admin admin = connection.getAdmin();
	admin.createTable(htd);
	
	//helper.fillTable("testtable", 1, 10, 10, "colfam1");
	


	Table table = connection.getTable(TableName.valueOf("testtable"));
	
	Get get = new Get(Bytes.toBytes("@@@GETTIME@@@") );
	get.addFamily(Bytes.toBytes("colfam1"));
	Result rs = table.get(get);
	System.out.println(rs);
	
	
	Put put = new Put(Bytes.toBytes("row-1"));
	put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("c4"), Bytes.toBytes("val4"));
	put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("c5"), Bytes.toBytes("val5"));
	put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("c6"), Bytes.toBytes("val6"));	
	put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("c44"), Bytes.toBytes("val44"));
	put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("c55"), Bytes.toBytes("val55"));
	put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("c66"), Bytes.toBytes("val66"));	
	put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("timestamp"), Bytes.toBytes(System.currentTimeMillis()));		
	table.put(put);	
	
	
	Put put1 = new Put(Bytes.toBytes("row-2"));
	put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("c4"), Bytes.toBytes("val4"));
	put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("c5"), Bytes.toBytes("val5"));
	put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("c6"), Bytes.toBytes("val6"));	
	put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("c44"), Bytes.toBytes("val44"));
	put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("c55"), Bytes.toBytes("val55"));
	put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("c66"), Bytes.toBytes("val66"));	
	put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("timestamp"), Bytes.toBytes(System.currentTimeMillis()));		
	table.put(put1);		
	
	table.close();
}
}
