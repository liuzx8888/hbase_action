package com.hbase.learn.hbase_action.ch04;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
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

public class RegionObserverExample2 extends BaseRegionObserver {
	public static final Log LOG = LogFactory.getLog(HRegion.class);

	public static final byte[] FIXED_ROW = Bytes.toBytes("@@@GETTIME@@@");

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

	@Override
	public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability)
			throws IOException {

		Configuration conf = HBaseConfiguration.create();
		HBaseHelper helper = HBaseHelper.getHelper(conf);
		Connection conn = helper.getConnection();
		TableName tableName = e.getEnvironment().getRegion().getTableDesc().getTableName();
		String idx_tableName = tableName.getNameAsString() + "_idx";

		if (!helper.existsTable(idx_tableName)) {
			helper.createTable(idx_tableName, "family");
		}

		Table table = conn.getTable(TableName.valueOf(idx_tableName));

		NavigableMap<byte[], List<Cell>> FamilyCells = put.getFamilyCellMap();
		for (Entry<byte[], List<Cell>> familyCell : FamilyCells.entrySet()) {
			List<Cell> cells = familyCell.getValue();
			for (Cell cell : cells) {
				String family = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
				String qualifier = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
						cell.getQualifierLength());
				if (qualifier.equalsIgnoreCase("timestamp")) {
					byte[] rowkey = cell.getRow();
					String rowkey_index = Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
							cell.getValueLength());

					Put indexPut = new Put(rowkey_index.getBytes());
					indexPut.addColumn("family".getBytes(), "qualifier".getBytes(), rowkey);
					table.put(indexPut);
				}
			}
		}
		table.close();
	}

}
