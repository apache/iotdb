package cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl;

import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.format.PageHeader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReaderByTimeStamp;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by zhangjinrui on 2017/12/26.
 */
public class SeriesChunkReaderByTimestampImpl extends SeriesChunkReader implements SeriesReaderByTimeStamp{

    private long currentTimestamp;

    public SeriesChunkReaderByTimestampImpl(InputStream seriesChunkInputStream, TSDataType dataType, CompressionTypeName compressionTypeName) {
        super(seriesChunkInputStream, dataType, compressionTypeName);
        currentTimestamp = Long.MIN_VALUE;
    }

    @Override
    public boolean pageSatisfied(PageHeader pageHeader) {
        long maxTimestamp = pageHeader.data_page_header.max_timestamp;
        //If minTimestamp > currentTimestamp, this page should NOT be skipped
        if (maxTimestamp < currentTimestamp || maxTimestamp < getMaxTombstoneTime()) {
            return false;
        }
        return true;
    }

    @Override
    public boolean timeValuePairSatisfied(TimeValuePair timeValuePair) {
        return timeValuePair.getTimestamp() >= currentTimestamp && timeValuePair.getTimestamp() > getMaxTombstoneTime();
    }
    
    public void setCurrentTimestamp(long currentTimestamp) {
        this.currentTimestamp = currentTimestamp;
        if(hasCachedTimeValuePair && cachedTimeValuePair.getTimestamp() < currentTimestamp){
            hasCachedTimeValuePair = false;
        }
    }

    @Override
    public TsPrimitiveType getValueInTimestamp(long timestamp) throws IOException {
        setCurrentTimestamp(timestamp);
        if(hasCachedTimeValuePair && cachedTimeValuePair.getTimestamp() == timestamp){
            hasCachedTimeValuePair = false;
            return cachedTimeValuePair.getValue();
        }
        while (hasNext()){
            cachedTimeValuePair = next();
            if(cachedTimeValuePair.getTimestamp() == timestamp){
                return cachedTimeValuePair.getValue();
            }
            else if(cachedTimeValuePair.getTimestamp() > timestamp){
                hasCachedTimeValuePair = true;
                return null;
            }
        }
        return null;
    }
}
