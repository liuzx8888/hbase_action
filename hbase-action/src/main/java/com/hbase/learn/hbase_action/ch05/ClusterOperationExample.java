package com.hbase.learn.hbase_action.ch05;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

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
		byte[]  startKey= regionInfos.get(0).getStartKey();
		
		RegionLocator locator = conn.getRegionLocator(tableName);
		HRegionLocation location = locator.getRegionLocation(startKey);
		ServerName currentServerName = location.getServerName();
		System.out.println("currentServerName :" + currentServerName);

		List<String> regionNames = new ArrayList<String>();
		Collection<ServerName>  serverNames = new ArrayList<ServerName>();

		ClusterStatus status = admin.getClusterStatus();
		serverNames =  status.getServers();
		regionNames.add(regionInfos.get(0).getRegionNameAsString());
		String Mv2serverName = serverNames.iterator().next().getServerName();

		// admin.compact(tableName);
		// admin.balancer(true);
		String EncodedName =location.getRegionInfo().getEncodedName();

		//admin.move("d314de21984d07c938d45674d4d5a370".getBytes(), Mv2serverName.getBytes());
        //admin.move(EncodedName.getBytes(),"hadoop4,16020,1562033872534".getBytes());
		
		
//		admin.unassign(regionNames.get(0).getBytes(), true);
//		admin.assign(regionNames.get(0).getBytes());


		
		
		table.close();
		conn.close();
		helper.close();

	}

}
