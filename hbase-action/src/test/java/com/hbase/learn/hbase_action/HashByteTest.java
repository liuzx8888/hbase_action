package com.hbase.learn.hbase_action;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Random;

import com.hbase.learn.hbase_action.common.RegionSaltSpilt;
import com.hbase.learn.hbase_action.common.RegionSpiltNum;
import com.hbase.learn.hbase_action.common.RowKeySaltUtil;

public class HashByteTest {

	final static long [] sizeTable = 
		{ 9, 99, 999, 9999, 99999, 999999, 9999999,99999999, 999999999
,9999999999L,99999999999L,999999999999L, 9999999999999L,99999999999999L,
999999999999999L,9999999999999999L,99999999999999999L,999999999999999999L,
Long.MAX_VALUE };
	
	
	public static int  calchash (byte a[], int offset, int length) {
	    if (a == null)
	      return 0;
	    int result = 1;
	    for (int i = offset; i < offset + length; i++) {
	        result = 31 * result + a[i];
	    }
	    return result;
	}
	
	
	public static int stringSize(long x) {
		for (int i=0; ; i++)
		if (x <= sizeTable[i])
		return i+1;
		}

	
	//https://blog.csdn.net/defonds/article/details/8782785
	public static void main(String[] args) throws IOException {
		  char[] A_z = {'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
	                'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'};
	        Random r = new Random();   
	        int regionNum =new RegionSpiltNum(8).regionNum();
	        for (int i = 0; i < 1; i++) { 
	        	//String rowkey = MD5Hash.getMD5AsHex(Bytes.toBytes(A_z[r.nextInt(A_z.length)])).substring(0, 8)+"-"+ String.valueOf(i);
	        	long rowkey =1563962917469L;//System.currentTimeMillis();

	        	//long rowkey =System.currentTimeMillis();
	        	int j = stringSize(rowkey);
	           	byte[] row_array = new byte[j];
	           //	row_array = ;
	           	ByteBuffer buffer = ByteBuffer.allocate(j); 
	           	buffer.putLong(0, rowkey);
	           	row_array=buffer.array();
				
				byte rowsalt = RowKeySaltUtil.rowkey_hash(row_array, 1,j-1 , 102);
				System.out.println(rowsalt);
				byte[] newRowkey =RegionSaltSpilt.newRowKey(row_array, rowsalt);	
	           	System.out.println(newRowkey.length);
	           	
				byte[] newrowkey1 = new byte[newRowkey.length-5];
				byte[] oldrowkey = newRowkey;
				System.arraycopy(oldrowkey, 5, newrowkey1, 0, oldrowkey.length-5);
				System.out.println(new String(newrowkey1));
				
	           	buffer.put(newrowkey1, 0, newrowkey1.length);
	            buffer.flip();       	
	           	System.out.println(buffer.getLong());

	        }
	}
	
	
}
