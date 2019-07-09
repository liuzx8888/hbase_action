package com.hbase.learn.hbase_action.ch05;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.quotas.QuotaSettings;
import org.apache.hadoop.hbase.quotas.QuotaSettingsFactory;
import org.apache.hadoop.hbase.quotas.ThrottleType;
import org.apache.hadoop.hbase.util.Bytes;

import com.hbase.learn.common.HBaseHelper;

public class QuotaExample {
	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		// conf.setInt("hbase.client.retries.number", 4);
		HBaseHelper helper = HBaseHelper.getHelper(conf);
		Connection conn = helper.getConnection();

		TableName tableName = TableName.valueOf("testtable_htd");
		Table table = conn.getTable(tableName);

		Admin admin = conn.getAdmin();
		QuotaSettingsFactory.unthrottleTable(tableName);
		//QuotaSettings qs = QuotaSettingsFactory.throttleTable(tableName, ThrottleType.READ_NUMBER, 1, TimeUnit.MINUTES);
		QuotaSettings qs =QuotaSettingsFactory.unthrottleTable(tableName);
		admin.setQuota(qs);
		Scan scan = new Scan();
		// scan.setCaching(1);
		scan.addFamily(Bytes.toBytes("colfam1"));
		ResultScanner scanner = table.getScanner(scan);
		int numRows = 0;
		try {
			for (Result res : scanner) {
				System.out.println(res);
				numRows++;
			}
		} catch (Exception e) {
			System.out.println("Error occurred: " + e.getMessage());
		}
		System.out.printf("Number of rows: " + numRows +"\n");

		RegionLocator locator = conn.getRegionLocator(tableName);
		HRegionLocation location = locator.getRegionLocation("row-1".getBytes());
		HRegionInfo regionInfo = location.getRegionInfo();
		System.out.println(
				"RegionName:  " + regionInfo.getRegionNameAsString() 
			    +"\n"+
			    "ServerName:  " + location.getServerName()
			    );

		scanner.close();
		admin.close();
		conn.close();
		helper.close();

	}

}
