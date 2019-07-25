package com.hbase.learn.hbase_action.ch04;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.util.Bytes;

import com.hbase.learn.hbase_action.common.HBaseHelper;

public class TestCoprocess2 {
public static void main(String[] args) throws Exception {
	Configuration conf = HBaseConfiguration.create();
	HBaseHelper helper = HBaseHelper.getHelper(conf);
	Connection connection = ConnectionFactory.createConnection(conf);
	Table table =  connection.getTable(TableName.valueOf("testtable_ep"));
	Scan scan = new Scan();
	scan.setMaxVersions(1);
	scan.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("c1"));
	Filter filter = new CustomFilterL("row-1", "row-3");
	scan.setFilter(filter);
	

	
}
}