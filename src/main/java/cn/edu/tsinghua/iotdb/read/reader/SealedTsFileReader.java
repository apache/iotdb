package cn.edu.tsinghua.iotdb.read.reader;

import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;

import java.io.IOException;

public class SealedTsFileReader implements SeriesReader{

    private int usedIntervalFileIndex;
    private SeriesReader singleTsFileReader;


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
