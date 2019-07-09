package com.hbase.learn.hbase_action;

import java.util.ArrayList;
import java.util.List;

import com.hbase.learn.hbase_action.ConsistentHash.HashFunc;
import com.hbase.learn.hbase_action.ch05.ConsistentHash_String;

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
	        
	        System.out.println(consistentHash.get("a"));
	        System.out.println(consistentHash.get("b"));  
	        System.out.println(consistentHash.get("d"));
		}
}
