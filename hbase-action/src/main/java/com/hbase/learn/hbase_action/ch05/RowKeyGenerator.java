package com.hbase.learn.hbase_action.ch05;

public interface RowKeyGenerator {
	  byte [] nextId();
}
