package cn.edu.tsinghua.iotdb.read.reader;

import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;

import java.io.IOException;

public class IoTDBQueryWithFilterReader implements SeriesReader {

    private SeriesFilter<?> seriesFilter;

    public IoTDBQueryWithFilterReader(SeriesFilter<?> seriesFilter) {

    }

    @Override
    public boolean hasNext() throws IOException {
        return false;
    }

    @Override
    public TimeValuePair next() throws IOException {
        return null;
    }

    @Override
    public void skipCurrentTimeValuePair() throws IOException {

    }

    @Override
    public void close() throws IOException {

    }
}
