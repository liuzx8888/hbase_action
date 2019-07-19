package com.hbase.learn.hbase_action;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

import com.hbase.learn.hbase_action.ConsistentHash.HashFunc;
import com.hbase.learn.hbase_action.ch05.ConsistentHash_String;
import com.hbase.learn.hbase_action.ch05.RegionConsistentHashSpilt;

public class TestMain {
	 public static void main(String[] args) {
	        List<String> regionlist = new ArrayList<String>();
	        regionlist.add("region1");
	        regionlist.add("region2");
	        regionlist.add("region3");
	        regionlist.add("region4");
	        regionlist.add("region5");	        
	        
	        ConsistentHash<String> consistentHash = new ConsistentHash<String>(2, regionlist);
//	        ConsistentHash_String<String> consistentHash = new ConsistentHash_String<String>(2, regionlist);   

	        System.out.println(consistentHash.hashFunc.getHashkey("region1"));
	        System.out.println(consistentHash.hashFunc.getHashkey("region2"));
	        System.out.println(consistentHash.hashFunc.getHashkey("region3"));
	        System.out.println(consistentHash.hashFunc.getHashkey("region4"));
	        System.out.println(consistentHash.hashFunc.getHashkey("region5"));
//     
//	        System.out.println("--------------------------------------------------------");
//	        
//	        System.out.println(consistentHash.hashFunc.getHashkey("a"));
//	        System.out.println(consistentHash.hashFunc.getHashkey("b"));
//	        System.out.println(consistentHash.hashFunc.getHashkey("c"));	        
//
//	        System.out.println("--------------------------------------------------------");	        
	        
//	        System.out.println(consistentHash.get("c")+   consistentHash.hashFunc.getHashkey("c"));
//	        System.out.println(consistentHash.get("a")+   consistentHash.get("a").hashCode() );  
//	        System.out.println(consistentHash.get("b")+   consistentHash.get("b").hashCode() );  
//	        System.out.println(consistentHash.get("dsds")+   consistentHash.get("sdsd").hashCode() );  	        
	        
//	        System.out.println(consistentHash.get("a"));
//	        System.out.println(consistentHash.get("b"));  
//	        System.out.println(consistentHash.get("d"));
	        char[] A_z = {'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
	                'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'};
	        Random r = new Random();   
	        
	        for (int i = 0; i < 10; i++) { 
	        	System.out.println(
						RegionConsistentHashSpilt.getRegion(
						  MD5Hash.getMD5AsHex(Bytes.toBytes(A_z[r.nextInt(A_z.length)])).substring(0,8))	
						  // +"-union-" 
						   +"-"
						   + MD5Hash.getMD5AsHex(Bytes.toBytes(A_z[r.nextInt(A_z.length)])).substring(0,8)						   
	        			) 
	        			;
	        
														
		}
}
	 }
