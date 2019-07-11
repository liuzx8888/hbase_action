package com.hbase.learn.hbase_action.ch04;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;

import com.hbase.learn.common.HBaseHelper;

public class LoadWithTableDescriptorExample {
	public static final Log LOG = LogFactory.getLog(LoadWithTableDescriptorExample.class);
	
	
	public static void main(String[] args) throws Exception {

		Configuration conf = HBaseConfiguration.create();
		HBaseHelper helper = HBaseHelper.getHelper(conf);
		Connection conn = helper.getConnection();
		helper.dropTable("testtable");

		HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("testtable"));
		htd.addFamily(new HColumnDescriptor("colfam1"));
//		htd.setValue("COPROCESSOR$1", "hdfs://hadoop1:8020/user/hbase/customCoprocessor/CustomSumendpoint.jar" + "|"
//				+ CustomEndPointService.class.getCanonicalName() + "|" + Coprocessor.PRIORITY_USER);
		htd.setValue("COPROCESSOR$1", "hdfs://hadoop1:8020/user/hbase/customCoprocessor/indexjar.jar" + "|"
				+  RegionObserverExample2.class.getCanonicalName() + "|" + Coprocessor.PRIORITY_USER);
		Admin admin = conn.getAdmin();
		admin.createTable(htd);
		
		System.out.println(admin.getTableDescriptor(TableName.valueOf("testtable")) );

		admin.close();
		conn.close();

	}

}
