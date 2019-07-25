package com.hbase.learn.hbase_action;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

import com.hbase.learn.hbase_action.common.RegionSaltSpilt;
import com.hbase.learn.hbase_action.common.RegionSpiltNum;
import com.hbase.learn.hbase_action.common.RowKeySaltUtil;

public class HashByteTest {

	public static int  calchash (byte a[], int offset, int length) {
	    if (a == null)
	      return 0;
	    int result = 1;
	    for (int i = offset; i < offset + length; i++) {
	        result = 31 * result + a[i];
	    }
	    return result;
	}
	
	
	
	
	
	public static void main(String[] args) throws IOException {
		  char[] A_z = {'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
	                'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'};
	        Random r = new Random();   
	        int regionNum =new RegionSpiltNum(8).regionNum();
	        for (int i = 0; i < 1000; i++) { 
	        	//String rowkey = MD5Hash.getMD5AsHex(Bytes.toBytes(A_z[r.nextInt(A_z.length)])).substring(0, 8)+"-"+ String.valueOf(i);
	        	
	        	System.out.println(Long.MAX_VALUE); //1563960847223
	        	String rowkey = Long.toString( 1563962917469L);
	        	
		
				byte rowsalt = RowKeySaltUtil.rowkey_hash(Bytes.toBytes(rowkey), 1, rowkey.length() - 1, regionNum);
				byte[] newbyterowkey = RowKeySaltUtil.new_rowkey_hash(rowsalt, rowkey);
				System.out.println("rowkey :" + rowkey  + "  rowsalt :"  +rowsalt  + "   newbyterowkey:" + new String (newbyterowkey));
				newbyterowkey = null;
	        }
	}
	
	
}
