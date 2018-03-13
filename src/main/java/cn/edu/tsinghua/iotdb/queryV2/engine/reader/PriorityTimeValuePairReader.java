package cn.edu.tsinghua.iotdb.queryV2.engine.reader;

import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.TimeValuePairReader;

import java.io.IOException;

/**
 * Created by zhangjinrui on 2018/1/11.
 */
public class PriorityTimeValuePairReader implements TimeValuePairReader {

    private TimeValuePairReader seriesReader;
    private Priority priority;

    public PriorityTimeValuePairReader(TimeValuePairReader seriesReader, Priority priority) {
        this.seriesReader = seriesReader;
        this.priority = priority;
    }

    @Override
    public boolean hasNext() throws IOException {
        return seriesReader.hasNext();
    }

    @Override
    public TimeValuePair next() throws IOException {
        return seriesReader.next();
    }

    @Override
    public void skipCurrentTimeValuePair() throws IOException {
        seriesReader.next();
    }

    @Override
    public void close() throws IOException {
        seriesReader.close();
    }

    public Priority getPriority() {
        return priority;
    }

    public static class Priority implements Comparable<Priority> {

        private int priority;

        public Priority(int priority) {
            this.priority = priority;
        }

        @Override
        public int compareTo(Priority o) {
            return this.priority - o.priority;
        }
    }
}
