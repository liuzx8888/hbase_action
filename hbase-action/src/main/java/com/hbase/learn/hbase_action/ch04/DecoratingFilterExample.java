package com.hbase.learn.hbase_action.ch04;

import java.io.IOException;

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
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SkipFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

import com.hbase.learn.common.HBaseHelper;

public class DecoratingFilterExample {
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		HBaseHelper helper = HBaseHelper.getHelper(conf);
//		helper.dropTable("testtable");
//		helper.createTable("testtable", "colfam1", "colfam2");
//		System.out.println("Adding rows to table...");
//		helper.fillTable("testtable", 1, 10, 10, "colfam1", "colfam2");
		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf("testtable"));
		
		
		Filter rowFilter = new RowFilter(CompareOp.NOT_EQUAL, new  BinaryComparator (Bytes.toBytes("row-3") )); 
		
		//Filter valueFilter = new ValueFilter(CompareOp.EQUAL, new  BinaryComparator (Bytes.toBytes("val-1.1") ));
		
		//Filter valueFilter = new ValueFilter(CompareOp.EQUAL, new SubstringComparator("val-1.1"));
		
		/*
		 * 满足条件的列整行数据被过滤
		 */
		SkipFilter skipFilter = new SkipFilter(rowFilter);
		
		/*
		 * 过滤出 全满足的条件
		 */
		
		WhileMatchFilter whileMatchFilter = new WhileMatchFilter(rowFilter);
		
		Scan scan = new Scan();
		scan.setFilter(whileMatchFilter);
		ResultScanner rss=  table.getScanner(scan);
		
		System.out.println("rowFilter:........................");	
		
		for (Result rs :rss) {
			for (Cell cell :rs.rawCells()) {
				System.out.println(   
					 " row:  " +Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength())
					+" fam : " + Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength())
					+" qua : " + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())
					+" timestamp : " + (cell.getTimestamp())						
					+" cellValue : " + ( cell.getValueLength() > 0 ? Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()) : "n/a"));
			}
		}
	}

}
