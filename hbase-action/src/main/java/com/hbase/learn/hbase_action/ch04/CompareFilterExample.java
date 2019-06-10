package com.hbase.learn.hbase_action.ch04;

import java.io.IOException;

import javax.swing.JList.DropLocation;

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
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.DependentColumnFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.hbase.learn.common.HBaseHelper;



public class CompareFilterExample {
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		HBaseHelper helper = HBaseHelper.getHelper(conf);
//		helper.dropTable("testtable");
//		helper.createTable("testtable", "colfam1", "colfam2");
//		System.out.println("Adding rows to table...");
//		helper.fillTable("testtable", 1, 10, 10, "colfam1", "colfam2");
		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf("testtable"));
		
		Filter valueFilter = new ValueFilter(CompareOp.EQUAL, new SubstringComparator(".6"));

		
		Scan scan = new Scan();
		scan.setFilter(valueFilter);
		ResultScanner rss=  table.getScanner(scan);
		System.out.println("valueFilter:...............");
		for (Result result : rss) {
		      for (Cell cell : result.rawCells()) {
		        System.out.println("Cell: " + cell + ", Value: " + // co ValueFilterExample-3-Print1 Print out value to check that filter works.
		         Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
		      }
		    }
		
		Filter rowFilter = new RowFilter(CompareOp.LESS_OR_EQUAL, new SubstringComparator("row-10"));
		Filter famFilter = new FamilyFilter(CompareOp.LESS_OR_EQUAL, new SubstringComparator("colfam1"));
		Filter quaFilter = new QualifierFilter(CompareOp.EQUAL,new SubstringComparator("col-6"));
		
		Filter DependentColumn =new DependentColumnFilter(Bytes.toBytes("colfam1"), Bytes.toBytes("col-6"), true);

		Filter DependentColumn1 = new DependentColumnFilter(Bytes.toBytes("colfam1"), Bytes.toBytes("col-6"), true, CompareOp.LESS ,new  SubstringComparator("val-9.6")  );
		
		Scan scan1 = new Scan();
		scan1.setFilter(rowFilter);
		scan1.setFilter(famFilter);
		scan1.setFilter(quaFilter);
		//scan1.setFilter(DependentColumn);	
		scan1.setFilter(DependentColumn1);	
		ResultScanner rss1=  table.getScanner(scan1);
		System.out.println("rowFilter:...............");	
		for (Result rs :rss1) {
			for (Cell cell :rs.rawCells()) {
				System.out.println(   
					 " Row:  " +Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength())
					+" fam :" + Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength())
					+" qua :" + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength())
					+" cellValue :" + Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
			}
		}
	}
	

	

}
