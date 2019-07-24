package com.hbase.learn.hbase_action.ch05;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.hash.Hash;

public class RegionSaltSpilt {

	public static int saltBucketNum = 1;

	public int getSaltBucketNum() {
		return saltBucketNum;
	}

	public RegionSaltSpilt (int saltBucketNum) {
		this.saltBucketNum = saltBucketNum;
	}

	public static byte[][] getSalteByteSplitPoints(int saltBucketNum) {
		byte[][] splits = new byte[saltBucketNum - 1][];
		for (int i = 1; i < saltBucketNum; i++) {
			splits[i - 1] = new byte[] { (byte) i };
		}
		return splits;
	}

	public static byte[] fillKey(byte[] key, int length) {
		if (key.length > length) {
			throw new IllegalStateException();
		}
		if (key.length == length) {
			return key;
		}
		byte[] newBound = new byte[length];
		System.arraycopy(key, 0, newBound, 0, key.length);
		return newBound;
	}

	public byte[][] splitRegionKey() {
		System.out.println("saltBucketNum:" + saltBucketNum);
		byte[][] splitkey = new byte[saltBucketNum  - 1][];
		byte[][] key_SplitPoints = this.getSalteByteSplitPoints(saltBucketNum);
		for (int i = 1; i < saltBucketNum ; i++) {
			byte[] key = key_SplitPoints[i-1];
			int length = 5;
			byte[] newBound = this.fillKey(key, length);
			splitkey[i-1] = newBound;
		}
		return splitkey;
	}
	
	public static byte[] newRowKey(byte[] oldrowkey,byte rowsalt) {
		byte[] newBound=RegionSaltSpilt.fillKey(new byte[] {rowsalt}, 5);
		byte[] byterowkey = oldrowkey;
		byte[] newbyterowkey  = new byte[newBound.length+byterowkey.length] ;
		System.arraycopy(newBound, 0, newbyterowkey, 0, newBound.length);
		System.arraycopy(byterowkey, 0, newbyterowkey, newBound.length, byterowkey.length);
		return newbyterowkey;
	}
	

}
