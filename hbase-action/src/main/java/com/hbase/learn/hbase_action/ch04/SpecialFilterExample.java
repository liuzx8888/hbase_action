package com.hbase.learn.hbase_action.ch04;

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
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RandomRowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.TimestampsFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.hbase.learn.common.HBaseHelper;

public class SpecialFilterExample {
  public static void main(String[] args) throws Exception {
	  Configuration conf = HBaseConfiguration.create();
		HBaseHelper helper = HBaseHelper.getHelper(conf);
//		helper.dropTable("testtable");
//		helper.createTable("testtable", "colfam1", "colfam2");
//		System.out.println("Adding rows to table...");
//		helper.fillTable("testtable", 1, 10, 10, "colfam1", "colfam2");
		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf("testtable"));
		
		
		/*
		 * 包含参照列
		 */
		SingleColumnValueFilter singleColumnValueFilter 
		= new SingleColumnValueFilter(Bytes.toBytes("colfam1"), Bytes.toBytes("col-4"), CompareOp.EQUAL, Bytes.toBytes("val-5.4"));
		singleColumnValueFilter.setFilterIfMissing(true);
		
		
		/*
		 * 不包含参照列
		 */
		SingleColumnValueExcludeFilter columnValueExcludeFilter
		= new SingleColumnValueExcludeFilter(Bytes.toBytes("colfam1"), Bytes.toBytes("col-4"), CompareOp.EQUAL, Bytes.toBytes("val-5.4"));
		columnValueExcludeFilter.setFilterIfMissing(true);
		
		
		
		PrefixFilter prefixFilter = new PrefixFilter(Bytes.toBytes("row-9"));
		MultipleColumnPrefixFilter multipleColumnPrefixFilter = new MultipleColumnPrefixFilter(new byte[][] {
	        Bytes.toBytes("col-2"), Bytes.toBytes("col-3")
	      });
		/*
		 * 显示 多少 列 ROW
		 */
		PageFilter pageFilter  = new PageFilter(5);
		
		/*
		 * 只返回 Key
		 */
		KeyOnlyFilter keyOnlyFilter = new KeyOnlyFilter(true);
		
		/*
		 * 只返回 每一列 第一行
		 */	
		FirstKeyOnlyFilter firstKeyOnlyFilter = new FirstKeyOnlyFilter();

		
		/*
		 * 包含结束行
		 */
		
		InclusiveStopFilter inclusiveStopFilter = new InclusiveStopFilter(Bytes.toBytes("row-3"));
		
		/*
		 * 时间戳过滤器
		 */		
		List<Long> timestamp = new ArrayList<Long>();
		timestamp.add(2L);
		timestamp.add(3L);
		
		TimestampsFilter timestampsFilter = new TimestampsFilter(timestamp);

		/*
		 * 列计数
		 * 类似返回 TOP 多少数据
		 */	
		ColumnCountGetFilter columnCountGetFilter =   new ColumnCountGetFilter(5);
		
		/*
		 * 列计数
		 * 类似 从第几ROW 第几个位置   开始开始返回 TOP 多少数据,
		 */	
		ColumnPaginationFilter columnPaginationFilter = new ColumnPaginationFilter(2, 8);
		
		/*
		 *根据 Column 名字 筛选数据 
		 */
		
		ColumnPrefixFilter columnPrefixFilter = new ColumnPrefixFilter(Bytes.toBytes("col-8"));

		
		/*
		 * 随机 筛选器
		 */
		RandomRowFilter randomRowFilter = new RandomRowFilter(0.5f);
		
	    Scan scan = new Scan();
		scan.setFilter(singleColumnValueFilter);
		scan.setFilter(columnValueExcludeFilter);
		scan.setFilter(prefixFilter);
		scan.setFilter(pageFilter);
		scan.setFilter(keyOnlyFilter);		
		scan.setFilter(firstKeyOnlyFilter);
		scan.setFilter(inclusiveStopFilter);
		scan.setStartRow(Bytes.toBytes("row-3"));
		
		scan.setFilter(timestampsFilter);
		scan.setFilter(columnCountGetFilter);
		scan.setFilter(columnPaginationFilter);		
		scan.setFilter(columnPrefixFilter);		
		scan.setFilter(multipleColumnPrefixFilter);
		scan.setFilter(randomRowFilter);
		
		/*
		 * 多个 过滤器  综合使用
		 */
		List<Filter> filters = new ArrayList<Filter>();
		filters.add(singleColumnValueFilter);
		filters.add(columnCountGetFilter);
		
		FilterList filterList = new FilterList(Operator.MUST_PASS_ALL, filters);
		
		scan.setFilter(filterList);
		
		table.getScanner(scan);
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
