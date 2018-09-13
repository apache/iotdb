package cn.edu.tsinghua.iotdb.queryV2.engine.reader.series;

import cn.edu.tsinghua.iotdb.queryV2.engine.overflow.OverflowOperation;
import cn.edu.tsinghua.iotdb.queryV2.engine.overflow.OverflowOperationReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReaderByTimeStamp;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.TimeValuePairReader;

import java.io.IOException;

/**
 * This class is used to combine one series with corresponding update operations
 * Created by zhangjinrui on 2018/1/15.
 */
public class SeriesWithOverflowOpReader implements SeriesReader {

    private TimeValuePairReader seriesReader;
    private OverflowOperationReader overflowOperationReader;
    private OverflowOperation overflowOperation;
    private boolean hasOverflowOperation;

    private TimeValuePair cachedTimeValuePair;
    private boolean hasCacheTimeValuePair;

    public SeriesWithOverflowOpReader(TimeValuePairReader seriesReader,
                                      OverflowOperationReader overflowOperationReader) throws IOException {
        this.seriesReader = seriesReader;
        this.overflowOperationReader = overflowOperationReader;
        if (overflowOperationReader.hasNext()) {
            overflowOperation = overflowOperationReader.next();
            hasOverflowOperation = true;
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        if (hasCacheTimeValuePair) {
            return true;
        }
        if (!seriesReader.hasNext()) {
            return false;
        }
        TimeValuePair timeValuePair = seriesReader.next();
        if (hasOverflowOperation) {
            long timestamp = timeValuePair.getTimestamp();
            hasOverflowOperation = moveToNextOperation(timestamp);
            if (satisfiedOperation(timestamp)) {
                if (overflowOperation.getType() == OverflowOperation.OperationType.UPDATE) {
                    timeValuePair.setValue(overflowOperation.getValue());
                    return cacheTimeValuePair(timeValuePair);
                } else if (overflowOperation.getType() == OverflowOperation.OperationType.DELETE) {
                    return hasNext();
                } else {
                    throw new UnsupportedOperationException("Unsupported overflow operation type:" + overflowOperation.getType());
                }
            } else {
                return cacheTimeValuePair(timeValuePair);
            }
        } else {
            return cacheTimeValuePair(timeValuePair);
        }
    }

    private boolean moveToNextOperation(long timestamp) {
        while (overflowOperation.getRightBound() < timestamp) {
            if (overflowOperationReader.hasNext()) {
                overflowOperation = overflowOperationReader.next();
            } else {
                return false;
            }
        }
        return true;
    }

    private boolean satisfiedOperation(long timestamp) {
        if (hasOverflowOperation &&
                overflowOperation.getLeftBound() <= timestamp && timestamp <= overflowOperation.getRightBound()) {
            return true;
        }
        return false;
    }

    private boolean cacheTimeValuePair(TimeValuePair timeValuePair) {
        cachedTimeValuePair = timeValuePair;
        hasCacheTimeValuePair = true;
        return true;
    }

    @Override
    public TimeValuePair next() throws IOException {
        if (hasNext()) {
            hasCacheTimeValuePair = false;
            return cachedTimeValuePair;
        }
        return null;
    }

    @Override
    public void skipCurrentTimeValuePair() throws IOException {
        next();
    }

    @Override
    public void close() throws IOException {
        seriesReader.close();
    }

    public TsPrimitiveType getValueInTimestamp(long timestamp) throws IOException {
        TsPrimitiveType value = ((SeriesReaderByTimeStamp)seriesReader).getValueInTimestamp(timestamp);
        if(value == null){
            return null;
        }
        if (hasOverflowOperation) {
            hasOverflowOperation = moveToNextOperation(timestamp);
            if (satisfiedOperation(timestamp)) {
                if (overflowOperation.getType() == OverflowOperation.OperationType.UPDATE) {
                    return overflowOperation.getValue();
                } else if (overflowOperation.getType() == OverflowOperation.OperationType.DELETE) {
                    return null;
                } else {
                    throw new UnsupportedOperationException("Unsupported overflow operation type:" + overflowOperation.getType());
                }
            } else {
                return value;
            }
        } else {
            return value;
        }
    }
}
