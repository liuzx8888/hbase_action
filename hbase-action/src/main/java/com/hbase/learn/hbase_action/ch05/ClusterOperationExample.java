package com.hbase.learn.hbase_action.ch05;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;

import com.hbase.learn.common.HBaseHelper;

public class ClusterOperationExample {
	public static void main(String[] args) throws IOException {

		Configuration conf = HBaseConfiguration.create();
		HBaseHelper helper = HBaseHelper.getHelper(conf);
		Connection conn = helper.getConnection();
		TableName tableName = TableName.valueOf("testtable_ep");
		Table table = conn.getTable(tableName);
		Admin admin = conn.getAdmin();

		List<HRegionInfo> regionInfos = admin.getTableRegions(tableName);

        List<String> regionNames = new ArrayList<String>();
        		
		for (HRegionInfo regionInfo : regionInfos) {
			regionNames.add(regionInfo.getRegionNameAsString());
			System.out.println(regionInfo.getRegionNameAsString());
		}
		admin.compact(tableName);
		admin.balancer(true);

		//testtable_ep,,1561366262458.4ff303dc03af0dc5a6d466205b82341b.
		for (String regionName :regionNames) {
			//admin.assign(regionName.getBytes());
			admin.unassign(regionName.getBytes(), true);
			admin.move(regionName.getBytes(), regionName.getBytes());
		}
		
		ClusterStatus status =  admin.getClusterStatus();
		
		System.out.println(status.toString());
		
		table.close();
		conn.close();
		helper.close();

	}

}
