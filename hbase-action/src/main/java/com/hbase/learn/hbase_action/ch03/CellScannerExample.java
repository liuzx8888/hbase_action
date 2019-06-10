package com.hbase.learn.hbase_action.ch03;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

public class CellScannerExample {
	public static void main(String[] args) throws Exception {
		Put put1 = new Put(Bytes.toBytes("row-1"));
		put1.addColumn(Bytes.toBytes("fam1"), Bytes.toBytes("c1"), Bytes.toBytes("val1"));

		Put put2 = new Put(Bytes.toBytes("row-2"));
		put2.addColumn(Bytes.toBytes("fam2"), Bytes.toBytes("c2"), Bytes.toBytes("val2"));

		Put put3 = new Put(Bytes.toBytes("row-3"));
		put3.addColumn(Bytes.toBytes("fam2"), Bytes.toBytes("c3"), Bytes.toBytes("val3"));

		Put put4 = new Put(Bytes.toBytes("row-1"));
		put4.addColumn(Bytes.toBytes("fam1"), Bytes.toBytes("c2"), Bytes.toBytes("val4"));		
		
		
		CellComparator comparator = new CellComparator.RowComparator();

		List<Cell> cells = new ArrayList<Cell>();

		Put[] puts = new Put[] { put1, put2, put3 , put4};

		for (Put put : puts) {
			CellScanner scanner = put.cellScanner();
			while (scanner.advance()) {
				Cell cell = scanner.current();
				cells.add(cell);
			}
		}

	    System.out.println("Shuffling...");
		Collections.shuffle(cells);
		for (Cell cell : cells) {
			System.out.println(cell);
			// System.out.println( new String( cell.getValue()));
		}

	    System.out.println("Sorting...");
		Collections.sort(cells, comparator);

		for (Cell cell : cells) {
			System.out.println(cell);
			// System.out.println( new String( cell.getValue()));
		}

	}

}
