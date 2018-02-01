package cn.edu.tsinghua.iotdb.service;

public interface MonitorMBean {
	long getDataSizeInByte();
	int getFileNodeNum();
	long getOverflowCacheSize();
	long getBufferWriteCacheSize();
	long getMergePeriodInSecond();
	long getClosePeriodInSecond();
	
	String getBaseDirectory();
	boolean getWriteAheadLogStatus();

	int getTotalOpenFileNum();
	int getDataOpenFileNum();
	int getWalOpenFileNum();
	int getDeltaOpenFileNum();
	int getDigestOpenFileNum();
	int getOverflowOpenFileNum();
	int getMetadataOpenFileNum();
	int getSocketOpenFileNum();
}
