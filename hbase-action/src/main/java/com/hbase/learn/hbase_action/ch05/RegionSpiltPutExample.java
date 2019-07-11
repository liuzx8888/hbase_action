package com.hbase.learn.hbase_action.ch05;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.BufferedMutator.ExceptionListener;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

import com.hbase.learn.common.HBaseHelper;
import com.hbase.learn.hbase_action.ch03.BufferedMutatorExample;

public class RegionSpiltPutExample {
	private static final Log LOG = LogFactory.getLog(RegionSpiltPutExample.class);

	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		HBaseHelper helper = HBaseHelper.getHelper(conf);

		Connection conn = helper.getConnection();

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
		params.writeBufferSize(123123L);
        char[] A_z = {'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
                'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'};
        Random r = new Random();
 
		try {
			BufferedMutator mutator = conn.getBufferedMutator(params);
			for (int i = 0; i < 1000; i++) {
				Put put1 = new Put(
						Bytes.toBytes(
								RegionConsistentHash.getRegion(
									MD5Hash.getMD5AsHex(Bytes.toBytes(A_z[r.nextInt(A_z.length)])).substring(0,8))	
								  // +"-union-" 
								   +"-"
								   + MD5Hash.getMD5AsHex(Bytes.toBytes(A_z[r.nextInt(A_z.length)])).substring(0,8)		
							   
								) 
								);
						
			
				put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("id"), Bytes.toBytes(String.valueOf(i)));
				put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("name"), Bytes.toBytes("test" + String.valueOf(i)));
				put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("sex"), Bytes.toBytes("male"));

				mutator.mutate(put1);
			}

		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		conn.close();
		helper.close();

	}
}
