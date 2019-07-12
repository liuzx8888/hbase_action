package com.hbase.learn.hbase_action.ch04;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.BufferedMutator.ExceptionListener;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import com.hbase.learn.common.HBaseHelper;
import com.hbase.learn.hbase_action.ch05.RegionConsistentHash;

public class RegionObserverExample2 extends BaseRegionObserver {
	public static final Log LOG = LogFactory.getLog(HRegion.class);

	public static final byte[] FIXED_ROW = Bytes.toBytes("@@@GETTIME@@@");

	private static Configuration conf;
	private static HBaseHelper helper;
	private static Connection conn;
	static {
		conf = HBaseConfiguration.create();
		try {
			helper = HBaseHelper.getHelper(conf);
			conn = helper.getConnection();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	@Override
	public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results)
			throws IOException {
	
		LOG.debug("Got preGet for row: " + Bytes.toStringBinary(get.getRow()));
		// vv RegionObserverWithBypassExample
		if (Bytes.equals(get.getRow(), FIXED_ROW)) {
			long time = System.currentTimeMillis();
			Cell cell = CellUtil.createCell(get.getRow(), FIXED_ROW, FIXED_ROW, // co
																				// RegionObserverWithBypassExample-1-Cell
																				// Create cell directly using the
																				// supplied utility.
					time, KeyValue.Type.Put.getCode(), Bytes.toBytes(time));
			// ^^ RegionObserverWithBypassExample
			LOG.debug("Had a match, adding fake cell: " + cell);
			// vv RegionObserverWithBypassExample
			results.add(cell);
			/* [ */e.bypass();/* ] */ // co RegionObserverWithBypassExample-2-Bypass Once the special cell is inserted
										// all subsequent coprocessors are skipped.
		}
		// ^^ RegionObserverWithBypassExample
	}

	@Override
	public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability)
			throws IOException {

		TableName tableName = e.getEnvironment().getRegion().getTableDesc().getTableName();
		String idx_tableName = tableName.getNameAsString() + "_idx";
	
		if (!helper.existsTable(idx_tableName)) {
			// helper.createTable(idx_tableName, "family");
			HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(idx_tableName));
			htd.addFamily(new HColumnDescriptor("family").setCompactionCompressionType(Algorithm.SNAPPY));
			Admin admin = conn.getAdmin();
			RegionConsistentHash consistentHash = new RegionConsistentHash();
			byte[][] regionspilt = RegionConsistentHash.splitRegionKey();
			admin.createTable(htd, regionspilt);
		}
		

//        String file = "hdfs://hadoop1:8020/user/hbase/customCoprocessor/RegionObserver.txt";
//		FileSystem fs = FileSystem.get(URI.create(file), conf);  
//		Path path = new Path(file);  
//		FSDataOutputStream out = fs.create(path);  
//	
//		out.write(  Bytes.toBytes("idx_tableName:"+ idx_tableName.toString()  + "table:"+   tableName));	
//		out.close();  
		
		
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

					 Put indexPut = new Put(
								 (  
								 RegionConsistentHash.getRegion(rowkey_index)
								 +'-'
								 +rowkey_index
								 )
							 .getBytes()
							 );
					 indexPut.addColumn("family".getBytes(), "qualifier".getBytes(), rowkey);
					 table.put(indexPut);
				}
			}
		}
		
//		table.close();
//		conn.close();
	}

	@Override
	public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit,
			Durability durability) throws IOException {
		// TODO Auto-generated method stub
		TableName tableName = e.getEnvironment().getRegion().getTableDesc().getTableName();
		String idx_tableName = tableName.getNameAsString() + "_idx";
		Table table = conn.getTable(TableName.valueOf(idx_tableName));
		byte[] rowkey = delete.getRow();


		if (helper.existsTable(idx_tableName)) {
			Delete del = new Delete(rowkey);
			table.delete(del);
		}
		table.close();
		conn.close();

	}
	


	
}
