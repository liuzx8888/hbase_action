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
		 stmt.executeUpdate("drop table testtable_htd");
/*
 * create table "TableName"("ROW" varchar primary key, "列簇"."列名" varchar , "列簇"."列名" varchar , "列簇"."列名" varchar);		
 */
//		stmt.executeUpdate("create table TEST (mykey integer not null primary key, mycolumn varchar) SALT_BUCKETS =10");
	//	stmt.executeUpdate("create table testtable_htd (rowkey integer not null primary key, colfam1.id varchar,colfam1.name varchar,colfam1.sex varchar,colfam1.timestamp varchar) ");

	//create table "testtable_htd" ("ROW" varchar primary key, "colfam1"."id" varchar , "colfam1"."name" varchar , "colfam1"."sex" varchar ,"colfam1"."timestamp" varchar) column_encoded_bytes=0;	
		
		 con.commit();

		 PreparedStatement statement = con.prepareStatement("select * from test");
		 rset = statement.executeQuery();
		 while (rset.next()) {
		 System.out.println(rset.getString("mycolumn"));
		 }
		 statement.close();
		con.close();
	}

}
