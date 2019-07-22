package com.hbase.learn.hbase_action.ch05;

public class RegionSpiltKey {
	/*
	 * @SpiltType Region Spilt类型 (1: 一致性 HashSpilt (需要传入服务器内存大小 G为单位) 2: 参考Phonix
	 * 加盐表 Spilt 方式)
	 */
	private int SpiltType ;
	private int RegionNum ;
	private byte[][] splitRegionKey = null;

	public RegionSpiltKey(int spiltType, int regionNum) {
		this.SpiltType = spiltType;
		this.RegionNum = regionNum;
	}

	
	
	
	public byte[][] splitRegionKey() {
		if (SpiltType == 1) {
			splitRegionKey = new RegionConsistentHashSpilt(RegionNum).splitRegionKey();
		} else if (SpiltType == 2) {
			splitRegionKey = new RegionSaltSpilt(RegionNum).splitRegionKey();
		}

		return splitRegionKey;

	}

}
