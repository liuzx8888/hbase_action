package com.hbase.learn.hbase_action.ch04;

import java.io.IOException;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.coprocessor.BaseRegionServerObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.Region;



public class RegionServerObserverExample extends BaseRegionServerObserver {
@Override
public void postMerge(ObserverContext<RegionServerCoprocessorEnvironment> c, Region regionA, Region regionB,
		Region mergedRegion) throws IOException {
	// TODO Auto-generated method stub
       
	HRegionInfo regionInfo =mergedRegion.getRegionInfo();
//  mergedRegion.getWAL().append(mergedRegion.getTableDesc(), regionInfo,
//  new WALKey(mergedRegion.getRegionName(), regionInfo.getTable()),
//  new WALEdit().add(CellUtil.createCell()))
	
}

}
