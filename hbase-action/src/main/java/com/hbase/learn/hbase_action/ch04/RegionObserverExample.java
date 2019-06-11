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
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import com.hbase.learn.common.HBaseHelper;

public class RegionObserverExample extends BaseRegionObserver {
	static Configuration conf;
	static HBaseHelper helper;
	static Connection conn;
	static {
		conf = HBaseConfiguration.create();

		try {
			helper = HBaseHelper.getHelper(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		conn = helper.getConnection();
	}

	public static final Log LOG = LogFactory.getLog(HRegion.class);

	public static final byte[] FIXED_ROW = Bytes.toBytes("@@@GETTIME@@@");

	@Override
	public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results)
			throws IOException {
		// TODO Auto-generated method stub

		if (Bytes.equals(get.getRow(), FIXED_ROW)) {
			Put put = new Put(get.getRow());
			put.addColumn(FIXED_ROW, FIXED_ROW, Bytes.toBytes(System.currentTimeMillis()));
			CellScanner cellScanner = put.cellScanner();
			cellScanner.advance();
			Cell cell = cellScanner.current();

			results.add(cell);

		}

	}

	@Override
	public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability)
			throws IOException {
		// TODO Auto-generated method stub
		byte[] rowkey = put.getRow();

		String tableName = e.getEnvironment().getRegion().getTableDesc().getTableName().getNameAsString();

		Table table_index = conn.getTable(TableName.valueOf(tableName + "_idx"));

		helper.dropTable(table_index.getName());
		helper.createTable(table_index.getName(), "info");

		Put put2 = new Put(Bytes.toBytes(System.currentTimeMillis()));
		put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes(tableName), rowkey);
		table_index.put(put2);
		table_index.close();
	}

}
