package org.apache.iotdb.tsfile.encoding.encoder.compressedsorter.datastructure;

public class CompressedSeriesData {
    CompressedData timeData;
    CompressedData valueData;
    long minTime;
    long maxTime;
    int pageTimeLen;
    int pageValueLen;
    boolean isInSorter;
    int count;
    public CompressedSeriesData(CompressedData t, CompressedData v, int totalNum, long minT, long maxT) {
        this.timeData = t;
        this.valueData = v;
        this.count = totalNum;
        this.minTime = minT;
        this.maxTime = maxT;
        this.isInSorter = true;
        this.pageTimeLen = t.getLen();
        this.pageValueLen = v.getLen();
    }

    public boolean getIsInSorter() {
        return this.isInSorter;
    }
    public long getMaxTime() {
        return this.maxTime;
    }

    public long getMinTime() {
        return this.minTime;
    }
    public int getPageTimeLen() {
        return this.pageTimeLen;
    }
    public int getPageValueLen() {
        return this.pageValueLen;
    }

    public int getCount() {
        return this.count;
    }

    public CompressedData getTimeData() {
        return this.timeData;
    }

    public CompressedData getValueData() {
        return this.valueData;
    }
}