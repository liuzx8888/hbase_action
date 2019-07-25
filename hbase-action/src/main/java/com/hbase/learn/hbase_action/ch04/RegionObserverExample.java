package com.hbase.learn.hbase_action.ch04;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import com.hbase.learn.hbase_action.common.HBaseHelper;

public class RegionObserverExample extends BaseRegionObserver {
	public static final Log LOG = LogFactory.getLog(HRegion.class);

	public static final byte[] FIXED_ROW = Bytes.toBytes("@@@GETTIME@@@");

//	public static HBaseHelper helper;
//	public static Connection conn;
//	public static HTable table;
//	
//	static {
//		Configuration conf = HBaseConfiguration.create();
//
//		try {
//			table = new HTable(conf, "guanzhu");
//			helper = HBaseHelper.getHelper(conf);
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		conn = helper.getConnection();
//
//	}

	@Override
	public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results)
			throws IOException {
		LOG.debug("Got preGet for row: " + Bytes.toStringBinary(get.getRow()));
		if (Bytes.equals(get.getRow(), FIXED_ROW)) {
			Put put = new Put(get.getRow());
			put.addColumn(FIXED_ROW, FIXED_ROW, Bytes.toBytes(System.currentTimeMillis()));
			CellScanner scanner = put.cellScanner();
			scanner.advance();
			Cell cell = scanner.current();
			LOG.debug("Had a match, adding fake cell: " + cell);
			results.add(cell);
		}
	}

//	@Override
//	public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability)
//			throws IOException {
//		LOG.debug("Got preGet for row: " + Bytes.toStringBinary(put.getRow()));
//		byte[] row = put.getRow();
//		Cell cell = put.get("f1".getBytes(), "from".getBytes()).get(0);
//		Put putIndex = new
//		Put(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
//		putIndex.addColumn("f1".getBytes(), "from".getBytes(), row);
//		table.put(putIndex);
//		table.close();
//
//	}
}
	