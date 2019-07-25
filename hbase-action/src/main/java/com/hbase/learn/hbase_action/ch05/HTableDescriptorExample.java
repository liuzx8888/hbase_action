package com.hbase.learn.hbase_action.ch05;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.query.QueryServicesOptions;
import org.apache.phoenix.util.SchemaUtil;

import com.hbase.learn.hbase_action.ch04.RegionObserverExample2;
import com.hbase.learn.hbase_action.common.HBaseHelper;

public class HTableDescriptorExample {

	static Configuration conf;
	static HBaseHelper helper;
	static Connection conn;

	static {

		try {
			conf = HBaseConfiguration.create();
			conf.set("hbase.client.ipc.pool.type", "RoundRobinPool");
			conf.set("hbase.client.ipc.pool.size", "50");
			conn =ConnectionFactory.createConnection(conf);
			helper = HBaseHelper.getHelper(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static void gettableRegion(String tableName) throws IOException {
		// TODO Auto-generated method stub
		TableName tn = TableName.valueOf(tableName);
		RegionLocator locator = conn.getRegionLocator(tn);
		Pair<byte[][], byte[][]> pair = locator.getStartEndKeys();

		
		for (int i = 0; i < pair.getFirst().length; i++) {
			byte[] sk = pair.getFirst()[i];
			byte[] ek = pair.getSecond()[i];
			System.out.println("[" + (i + 1) + "]" + " start key: "
					+ (sk.length == 8 ? Bytes.toLong(sk) : Bytes.toStringBinary(sk)) +
					", end key: " + (ek.length == 8 ? Bytes.toLong(ek) : Bytes.toStringBinary(ek)));
		}
		locator.close();

	}

	public static void main(String[] args) throws IOException {
        String table_name ="testtable_htd";
		helper.dropTable(table_name);
		//helper.dropTable("testtable_htd_idx");		
		HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(table_name));
	
		//alter 'testtable' ,METHOD=>'table_att','coprocessor$1'=>'hdfs:///hadoop1:8020/user/hbase/customCoprocessor/indexjar.jar|com.hbase.learn.hbase_action.ch04.RegionObserverExample2|1001'
		if (table_name.equals("testtable_htd")) {
			htd.addFamily(
					new HColumnDescriptor("colfam1")
					//.setValue("test_key", "test_value")
					//.setBloomFilterType(BloomType.ROW)
					.setCompactionCompressionType(Algorithm.SNAPPY)
					);
			htd.setValue("COPROCESSOR$1", "hdfs://hadoop1:8020/user/hbase/customCoprocessor/indexjar.jar" + "|"
				+  RegionObserverExample2.class.getCanonicalName() + "|" + Coprocessor.PRIORITY_USER);
		}
		
		if (table_name.equals("testtable_htd_idx")) {
		htd.addFamily(
				new HColumnDescriptor("family")
				//.setValue("test_key", "test_value")
				//.setBloomFilterType(BloomType.ROW)
				.setCompactionCompressionType(Algorithm.SNAPPY)
				);
		}
		
		Admin admin = conn.getAdmin();
		System.out.println("ServersSize :  " +admin.getClusterStatus().getServersSize());
		/*
		 * 增加 Namespace shell 查看命令 list_namespace_tables 'Test'
		 */
		if (admin.getNamespaceDescriptor("Test") == null) {
			admin.createNamespace(NamespaceDescriptor.create("Test").build());
		}
		// admin.createTable(htd);

		/*
		 * 预分区
		 * 两种方案  
		 * 	admin.createTable(desc, startKey, endKey, numRegions);
		 * 	admin.createTable(desc, splitKeys);
		 */
		//admin.createTable(htd, null, null, 20);
//		 admin.createTable(htd, Bytes.toBytes("region_1"), Bytes.toBytes("region_10"), 10);
//		byte[][] regions= new byte[][] {
//			  Bytes.toBytes("A"),
//		      Bytes.toBytes("D"),
//		      Bytes.toBytes("G"),
//		      Bytes.toBytes("K"),
//		      Bytes.toBytes("O"),
//		      Bytes.toBytes("T")  
//		};
		

		 
	    int RegionNum = new RegionSpiltNum(8).regionNum();
		byte[][] regionspilt= new RegionSpiltKey(2, RegionNum).splitRegionKey();
		admin.createTable(htd, regionspilt);
		gettableRegion(table_name);
		
		/*
		 * 获取List Table
		 */
		HTableDescriptor[] htds = admin.listTables(".*",true);
		HTableDescriptor[] htds1 = admin.listTables("test.*",true);
		HTableDescriptor[] htds2 = admin.listTables("hbase.*",true);	
		HTableDescriptor[] htds3 =admin.listTableDescriptorsByNamespace("Test");
		TableName[] tables =admin.listTableNames("test.*");
		
		
		for(int i=0;i<htds.length;i++) {
		  System.out.println(" List Table:  " + htds[i].getTableName().getNameAsString());	
		}
		
		for(int i=0;i<htds1.length;i++) {
			  System.out.println(" List test Table:  " + htds1[i].getTableName().getNameAsString());	
			}	
		for(int i=0;i<htds2.length;i++) {
			  System.out.println(" List hbase Table:  " + htds2[i].getTableName().getNameAsString());	
			}	
		for(int i=0;i<htds3.length;i++) {
			  System.out.println(" List testNamespace Table:  " + htds3[i].getTableName().getNameAsString());	
			}			
	
		
		
		/*
		 * 查看表状态
		 */
		admin.isTableAvailable(TableName.valueOf(table_name));
		System.out.println(admin.getTableDescriptor(TableName.valueOf(table_name)));

		
		admin.close();
		conn.close();
		helper.close();

	}

}
