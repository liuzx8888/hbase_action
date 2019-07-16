package com.hbase.learn.hbase_action.ch04;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.hash.Hash;

public class RegionSaltRowkey {
		static int SALT_BUCKETS =8;
	
		
		public static int hash (byte a[], int offset, int length) {
		    if (a == null)
		      return 0;
		    int result = 1;
		    for (int i = offset; i < offset + length; i++) {
		        result = 31 * result + a[i];
		    }
		    return result;
		}	
		
	public static void main(String[] args) {
		for (int i=0;i<8;i++) {
		
			System.out.println( );
			
		}
		
		
	}

}
