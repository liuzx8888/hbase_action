package com.hbase.learn.hbase_action.common;

import org.apache.hadoop.hbase.util.Bytes;

public class RowKeySaltUtil {
	static byte[] newBound = null;
	static byte[] byterowkey = null;
	static byte[] newbyterowkey = null;

	public static int calchash(byte a[], int offset, int length) {
		if (a == null)
			return 0;
		int result = 1;
		for (int i = offset; i < offset + length; i++) {
			result = 31 * result + a[i];
		}
		return result;
	}

	public static byte rowkey_hash(byte a[], int offset, int length, int regionNum) {

		int calchash = calchash(a, offset, length);
		byte rerutnHash = (byte) Math.abs(calchash % regionNum);
		return rerutnHash;

	}

	public static byte[] new_rowkey_hash(byte rowsalt, String rowkey) {
		newBound = byterowkey = newbyterowkey = null;
		newBound = RegionSaltSpilt.fillKey(new byte[] { rowsalt }, 5);
		byterowkey = Bytes.toBytes(rowkey);
		newbyterowkey = new byte[newBound.length + byterowkey.length];
		System.arraycopy(newBound, 0, newbyterowkey, 0, newBound.length);
		System.arraycopy(byterowkey, 0, newbyterowkey, newBound.length, byterowkey.length);
		return newbyterowkey;

	}

}
