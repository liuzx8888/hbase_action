package com.hbase.learn.hbase_action.ch05;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutator.ExceptionListener;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

import com.hbase.learn.hbase_action.common.HBaseHelper;
import com.hbase.learn.hbase_action.common.RegionSaltSpilt;
import com.hbase.learn.hbase_action.common.RegionSpiltNum;
import com.hbase.learn.hbase_action.common.RowKeySaltUtil;

public class RegionSpiltPutExample {
	private static final Log LOG = LogFactory.getLog(RegionSpiltPutExample.class);

	public static void main(String[] args) throws IOException {
		System.out.println(System.currentTimeMillis());
		Configuration conf = HBaseConfiguration.create();
		HBaseHelper helper = HBaseHelper.getHelper(conf);
		conf.set("hbase.client.ipc.pool.type", "RoundRobinPool");
		conf.set("hbase.client.ipc.pool.size", "50");
		Connection conn =ConnectionFactory.createConnection(conf);
		helper = HBaseHelper.getHelper(conf);

		TableName tn = TableName.valueOf("testtable_htd");
		Table table = conn.getTable(tn);
        Random random = new Random();
		
		
    	BufferedMutator.ExceptionListener listener = new ExceptionListener() {
			@Override
			public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator)
					throws RetriesExhaustedWithDetailsException {
				// TODO Auto-generated method stub
				for (int i = 0; i < e.getNumExceptions(); i++) {
					LOG.info("Failed to sent put " + e.getRow(i) + ".");
				}

			}
		};
		BufferedMutatorParams params = new BufferedMutatorParams(table.getName()).listener(listener);
		params.writeBufferSize(123L);
		
		

        char[] A_z = {'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
                'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'};
        Random r = new Random();
		int regionNum = new RegionSpiltNum(8).regionNum();
		try {

			BufferedMutator mutator = conn.getBufferedMutator(params);
			for (int i = 1; i < 1000; i++) {
				String rowkey = MD5Hash.getMD5AsHex(Bytes.toBytes(A_z[r.nextInt(A_z.length)])).substring(0, 8)+"-"+ String.valueOf(i);
				
				byte rowsalt = RowKeySaltUtil.rowkey_hash(Bytes.toBytes(rowkey), 1, rowkey.length() - 1, regionNum);
				byte[] newBound=RegionSaltSpilt.fillKey(new byte[] {rowsalt}, 5);
				byte[] byterowkey = Bytes.toBytes(rowkey);
				
				byte[] newbyterowkey  = new byte[newBound.length+byterowkey.length] ;
				System.arraycopy(newBound, 0, newbyterowkey, 0, newBound.length);
				System.arraycopy(byterowkey, 0, newbyterowkey, newBound.length, byterowkey.length);
				
	
				Put put1 = new Put(newbyterowkey);
				put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("id"), Bytes.toBytes(String.valueOf(i)));
				put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("name"), Bytes.toBytes("test" + String.valueOf(i)));
				put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("sex"), Bytes.toBytes("male"));
				put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("partition"), Bytes.toBytes(rowsalt));				
				//put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("timestamp"), Bytes.toBytes(String.valueOf(System.currentTimeMillis()+i)));
				
				mutator.mutate(put1);
				//table.put(put1);
				newBound=null;
				byterowkey = null;
				newbyterowkey = null;
			}

		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		table.close();
		conn.close();
		helper.close();
	}
}
