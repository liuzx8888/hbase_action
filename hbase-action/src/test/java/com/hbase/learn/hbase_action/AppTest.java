package com.hbase.learn.hbase_action;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.util.SchemaUtil;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest extends TestCase {
	/**
	 * Create the test case
	 *
	 * @param testName
	 *            name of the test case
	 */
	public AppTest() {
	}

	public AppTest(String testName) {
		super(testName);
	}

	/**
	 * @return the suite of tests being tested
	 */
	public static Test suite() {
		return new TestSuite(AppTest.class);
	}

	/**
	 * Rigourous Test :-)
	 */
	public void testApp() {
		assertTrue(true);
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

	public static void main(String[] args) {
		List<String> regionlist = new ArrayList<String>();
		regionlist.add("a");
		regionlist.add("b");
		regionlist.add("c");

		ConsistentHash<String> consistentHash = new ConsistentHash<>(10, regionlist);
		System.out.println(consistentHash.hashFunc.toString());
		System.out.println(new Random().nextInt(50000000));

		AppTest test = new AppTest();

		byte[][] key1 = test.getSalteByteSplitPoints(10);

		for (int i = 0; i < 9; i++) {
			byte[] key = key1[i];
			int length = 5;
			byte[] newBound = test.fillKey(key, length);
			System.out.println(newBound);
		}

	}

}
