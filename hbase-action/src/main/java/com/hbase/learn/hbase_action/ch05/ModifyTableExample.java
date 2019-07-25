package com.hbase.learn.hbase_action.ch05;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.hbase.learn.hbase_action.common.HBaseHelper;

public class ModifyTableExample {
	static Configuration conf;
	static HBaseHelper helper;
	static Connection conn;

	static {

		try {
			conf = HBaseConfiguration.create();
			helper = HBaseHelper.getHelper(conf);
			conn = helper.getConnection();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static void gettableRegion(String tableName) throws IOException {
		// TODO Auto-generated method stub
		TableName tn = TableName.valueOf(tableName);
		RegionLocator locator = conn.getRegionLocator(tn);
		Pair<byte[][], byte[][]> pair = locator.getStartEndKeys();

		for (int i = 0; i < pair.getFirst().length; i++) {
			byte[] sk = pair.getFirst()[i];
			byte[] ek = pair.getSecond()[i];
			System.out.println("[" + (i + 1) + "]" + " start key: "
					+ (sk.length == 8 ? Bytes.toLong(sk) : Bytes.toStringBinary(sk)) + ", end key: "
					+ (ek.length == 8 ? Bytes.toLong(ek) : Bytes.toStringBinary(ek)));
		}
		locator.close();

	}

	public static void main(String[] args) throws IOException, InterruptedException {
		/*
		 * 增加表结构
		 */
		Admin admin1 = conn.getAdmin();
		/*
		 * new HTableDescriptor(TableName.valueOf("testtable_htd")) 相当于重新创建表
		 * admin1.getTableDescriptor 获取当前表，在当前表的基础上修改 表结构
		 */
		HTableDescriptor htd1 = admin1.getTableDescriptor(TableName.valueOf("testtable_htd"));
		htd1.addFamily(new HColumnDescriptor(Bytes.toBytes("colfam2")));
	
		admin1.disableTable(TableName.valueOf("testtable_htd"));
		admin1.modifyTable(TableName.valueOf("testtable_htd"), htd1);

		Pair<Integer, Integer> status = new Pair<Integer, Integer>() {
			{
				setFirst(50);
				setSecond(50);
			}
		};
		
		
		for (int i = 0; status.getFirst() != 0 && i < 500; i++) {
			status = admin1.getAlterStatus(TableName.valueOf("testtable_htd"));
			   Thread.sleep(1 * 1000l);
		}
		gettableRegion("testtable_htd");
		
		admin1.enableTable(TableName.valueOf("testtable_htd"));	
		/*
		 * 查看表状态
		 */
		admin1.isTableAvailable(TableName.valueOf("testtable_htd"));
		System.out.println(admin1.getTableDescriptor(TableName.valueOf("testtable_htd")));
		
		admin1.close();
		conn.close();
		helper.close();

	}
	
}
