package com.hbase.learn.hbase_action;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp()
    {
        assertTrue( true );
    }
    
    public static void main(String[] args) {
        List<String> regionlist = new ArrayList<String>();
        regionlist.add("a");
        regionlist.add("b");
        regionlist.add("c");     
       
        ConsistentHash<String> consistentHash = new ConsistentHash<>(10, regionlist);
        System.out.println(consistentHash.hashFunc.toString());
	}
    
    
}
