package com.hbase.learn.hbase_action;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;

import org.antlr.runtime.ANTLRReaderStream;
public class Testphoenix {
	public static void main(String[] args) throws SQLException, InterruptedException {
		

		Statement stmt = null;
        ResultSet rset = null;
        
        Connection con = DriverManager.getConnection("jdbc:phoenix:hadoop1:2181");
        stmt = con.createStatement();
        stmt.executeUpdate("drop table test");
        stmt.executeUpdate("create table test (mykey integer not null primary key, mycolumn varchar) SALT_BUCKETS =10");
 
        

       // con.commit();
        
//        PreparedStatement statement = con.prepareStatement("select * from test");
//        rset = statement.executeQuery();
//        while (rset.next()) {
//            System.out.println(rset.getString("mycolumn"));
//        }
//        statement.close();
        con.close();
    }

}


