package com.hbase.learn.hbase_action.ch03;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class GetExample {
	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, "test_table");
		Get get = new Get(Bytes.toBytes("row2"));
		// 精确到某 columnfamily 中的 qualifier
		get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("c7"));
		// 获取RowKey
		byte[] rows = get.getRow();
		System.out.println(new String(rows));

		Get get12 = new Get(Bytes.toBytes("row2"));
		// 精确到某 columnfamily 中的 qualifier
		get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("c7"));
		// 获取RowKey
		byte[] rows12 = get12.getRow();
		System.out.println(new String(rows12));

		// 获取时间戳
		String tr = get.getTimeRange().toString();
		System.out.println(String.valueOf(tr));
  
		Result result = table.get(get);
		
		System.out.println(result.getExists());

		// 获取多行记录
		byte[] fam = Bytes.toBytes("colfam1");
		byte[] c1 = Bytes.toBytes("c1");
		byte[] c2 = Bytes.toBytes("c2");
		byte[] c3 = Bytes.toBytes("c3");
		byte[] c4 = Bytes.toBytes("c4");
		byte[] row1 = Bytes.toBytes("row1");
		byte[] row2 = Bytes.toBytes("row2");
		byte[] row11 = Bytes.toBytes("row11");
		List<Get> gets = new ArrayList<Get>();

		Get get00 = new Get(row11);
		get00.addColumn(fam, c1);
		// 判斷是否存在
		if (table.exists(get00)) {
			gets.add(get00);
		}

		Get get11 = new Get(row1);
		get11.addColumn(fam, c1);
		if (table.exists(get11)) {
			gets.add(get11);
		}

		Get get22 = new Get(row1);
		get22.addColumn(fam, c2);
		gets.add(get22);

		Get get33 = new Get(row1);
		get33.addColumn(fam, c3);
		gets.add(get33);

		Get get44 = new Get(row2);
		get44.addColumn(fam, c4);
		gets.add(get44);

		Result[] results = table.get(gets);

		for (Result rs1 : results) {
			// System.out.println(rs1.toString());
			if (rs1.containsColumn(fam, c1)) {
				System.out.println(rs1.toString());
			}
		}

		/// RAW Key-value
		for (Result rs1 : results) {
			for (KeyValue kv : rs1.raw()) {
				System.out.println(Bytes.toString(kv.getQualifier()) + Bytes.toString(kv.getFamily()));
			}
		}

	}
}
