package com.hbase.learn.hbase_action.ch03;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import com.hbase.learn.common.HBaseHelper;


public class CheckandPutTest {
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		//conf.set("hbase.zookeeper.quorum", "132.35.81.207");
		//conf.set("zookeeper.znode.parent", "/hbase1");

	    HBaseHelper helper = HBaseHelper.getHelper(conf);
	    Connection connection = helper.getConnection();
		if (helper.existsTable("test_table")) {
			helper.dropTable("test_table");
		}
		helper.createTable("test_table", "colfam1");
		HTable table = new HTable(conf, "test_table");

		Put put1 = new Put(Bytes.toBytes("row1"));
		put1.addColumn (Bytes.toBytes("colfam1"), Bytes.toBytes("col1"), Bytes.toBytes("val1"));

		// check一下，如果改列有没有值，则执行put操作。
		boolean res1 = table.checkAndPut(Bytes.toBytes("row1"), Bytes.toBytes("colfam1"), Bytes.toBytes("col1"), null,
				put1);

		// 输出结果看是否执行了put
		System.out.println("Put applied1:" + res1);

		// 再次put一条同样的记录
		boolean res2 = table.checkAndPut(Bytes.toBytes("row1"), Bytes.toBytes("colfam1"), Bytes.toBytes("col1"), null,
				put1);

		// 输出执行结果（应当返回false）
		System.out.println("Put applied2:" + res2);

		Put put2 = new Put(Bytes.toBytes("row1"));
		put2.add(Bytes.toBytes("colfam1"), Bytes.toBytes("col2"), Bytes.toBytes("val2"));

		// check一下，如果之前val1录入成功，则录入新值
		boolean res3 = table.checkAndPut(Bytes.toBytes("row1"), Bytes.toBytes("colfam1"), Bytes.toBytes("col1"),
				Bytes.toBytes("val1"), put2);

		// 输出执行结果，看是否执行了Put
		System.out.println("Put applied3:" + res3);

		Put put3 = new Put(Bytes.toBytes("row2"));
		put3.add(Bytes.toBytes("colfam1"), Bytes.toBytes("col1"), Bytes.toBytes("val3"));

		// check一下，row1的值是否存在，如果存在则插入row2
		// 会抛出异常(org.apache.hadoop.hbase.DoNotRetryIOException: Action's getRow must
		// match the passed row)
		boolean res4 = table.checkAndPut(Bytes.toBytes("row1"), Bytes.toBytes("colfam1"), Bytes.toBytes("col1"),
				Bytes.toBytes("val1"), put3);

		// 输出执行结果，看是否执行了Put
		System.out.println("Put applied4: " + res4);
	}

}
