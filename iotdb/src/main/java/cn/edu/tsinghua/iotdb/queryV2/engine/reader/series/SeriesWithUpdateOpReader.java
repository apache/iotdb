package cn.edu.tsinghua.iotdb.queryV2.engine.reader.series;

import cn.edu.tsinghua.iotdb.queryV2.engine.overflow.OverflowOperation;
import cn.edu.tsinghua.iotdb.queryV2.engine.overflow.OverflowOperationReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.TimeValuePairReader;

import java.io.IOException;

/**
 * This class is used to combine one series with corresponding update operations
 * Created by zhangjinrui on 2018/1/15.
 */
public class SeriesWithUpdateOpReader implements SeriesReader {

    private TimeValuePairReader seriesReader;
    private OverflowOperationReader overflowOperationReader;
    private OverflowOperation overflowUpdateOperation;
    private boolean hasOverflowUpdateOperation;

    public SeriesWithUpdateOpReader(TimeValuePairReader seriesReader,
                                    OverflowOperationReader overflowOperationReader) throws IOException {
        this.seriesReader = seriesReader;
        this.overflowOperationReader = overflowOperationReader;
        if (overflowOperationReader.hasNext()) {
            overflowUpdateOperation = overflowOperationReader.next();
            hasOverflowUpdateOperation = true;
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        return seriesReader.hasNext();
    }

    @Override
    public TimeValuePair next() throws IOException {
        TimeValuePair timeValuePair = seriesReader.next();
        if (hasOverflowUpdateOperation) {
            long timestamp = timeValuePair.getTimestamp();
            while (overflowUpdateOperation.getRightBound() < timestamp) {
                if (overflowOperationReader.hasNext()) {
                    overflowUpdateOperation = overflowOperationReader.next();
                    hasOverflowUpdateOperation = true;
                } else {
                    hasOverflowUpdateOperation = false;
                    break;
                }
            }
            if (hasOverflowUpdateOperation &&
                    overflowUpdateOperation.getLeftBound() <= timestamp && timestamp <= overflowUpdateOperation.getRightBound()) {
                timeValuePair.setValue(overflowUpdateOperation.getValue());
            }
        }
        return timeValuePair;
    }

    @Override
    public void skipCurrentTimeValuePair() throws IOException {
        next();
    }

    @Override
    public void close() throws IOException {
        seriesReader.close();
    }
}
