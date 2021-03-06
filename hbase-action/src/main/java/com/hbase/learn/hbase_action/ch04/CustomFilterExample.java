package com.hbase.learn.hbase_action.ch04;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SkipFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

import com.hbase.learn.hbase_action.common.HBaseHelper;



public class CustomFilterExample {
      public static void main(String[] args) throws Exception {
		
    	  Configuration conf = HBaseConfiguration.create();
  	    HBaseHelper helper = HBaseHelper.getHelper(conf);
//  	    helper.dropTable("testtable");
//  	    helper.createTable("testtable", "colfam1");
//  	    System.out.println("Adding rows to table...");
//  	    helper.fillTable("testtable", 1, 10, 10, 2, true, "colfam1");

  	    Connection connection = ConnectionFactory.createConnection(conf);
  	    Table table = connection.getTable(TableName.valueOf("testtable_ep"));
  	    // vv CustomFilterExample
  	    List<Filter> filters = new ArrayList<Filter>();

//  	    Filter filter1 = new CustomFilter(Bytes.toBytes("val-05.05"));
//  	    filters.add(filter1);
//
//  	    Filter filter2 = new CustomFilter(Bytes.toBytes("val-02.07"));
//  	    filters.add(filter2);
//
//  	    Filter filter3 = new CustomFilter(Bytes.toBytes("val-09.01"));
//  	    filters.add(filter3);
//
//  	    FilterList filterList = new FilterList(
//  	      FilterList.Operator.MUST_PASS_ONE, filters);

  	    Filter filterL = new CustomFilterL("row-1","row-6");

  	    
  	    Scan scan = new Scan();


  	    scan.setFilter(filterL);
  	    ResultScanner scanner = table.getScanner(scan);
  	    // ^^ CustomFilterExample
  	    System.out.println("Results of scan:");
  	    // vv CustomFilterExample
  	    for (Result result : scanner) {
  	      for (Cell cell : result.rawCells()) {
  	        System.out.println(
  	        		"Cell: " + cell + ", Value: " +
  	          Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
  	            cell.getValueLength()) +
  	    	       ", Rowkey: " +  new String(cell.getRow())
  	        		);
  	      }
  	    }
  	    scanner.close();
  	    // ^^ CustomFilterExample
    	  
	}
}
