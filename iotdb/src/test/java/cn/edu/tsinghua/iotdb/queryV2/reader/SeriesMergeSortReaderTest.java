package cn.edu.tsinghua.iotdb.queryV2.reader;

import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityTimeValuePairReader;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityTimeValuePairReader.Priority;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityMergeSortTimeValuePairReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhangjinrui on 2018/1/11.
 */
public class SeriesMergeSortReaderTest {

    @Test
    public void test2S() throws IOException {
        //2 series
        test(new long[]{1, 2, 3, 4, 5, 6}, new long[]{2, 2, 2, 1, 2, 2}, new long[]{1, 2, 3, 4, 5}, new long[]{1, 2, 3, 5, 6});
        test(new long[]{1, 2, 3, 4, 5}, new long[]{1, 1, 1, 1, 1}, new long[]{1, 2, 3, 4, 5}, new long[]{});
        test(new long[]{1, 2, 3, 4, 5}, new long[]{2, 2, 2, 2, 2}, new long[]{}, new long[]{1, 2, 3, 4, 5});
        test(new long[]{1, 2, 3, 4, 5, 6, 7, 8}, new long[]{1, 1, 1, 1, 1, 2, 2, 2}, new long[]{1, 2, 3, 4, 5}, new long[]{6, 7, 8});

        //3 series
        test(new long[]{1, 2, 3, 4, 5, 6, 7}, new long[]{3, 3, 3, 1, 3, 2, 3}, new long[]{1, 2, 3, 4, 5}, new long[]{1, 2, 3, 5, 6}, new long[]{1, 2, 3, 5, 7});
        test(new long[]{1, 2, 3, 4, 5, 6}, new long[]{1, 1, 2, 3, 2, 3}, new long[]{1, 2}, new long[]{3, 5}, new long[]{4, 6});
    }

    private void test(long[] retTimestamp, long[] retValue, long[]... sources) throws IOException {
        List<PriorityTimeValuePairReader> priorityTimeValuePairReaders = new ArrayList<>();
        for (int i = 0; i < sources.length; i++) {
            priorityTimeValuePairReaders.add(new PriorityTimeValuePairReader(new FakedSeriesReader(sources[i], i + 1), new Priority(i + 1)));
        }
        PriorityMergeSortTimeValuePairReader seriesMergeSortReader = new PriorityMergeSortTimeValuePairReader(priorityTimeValuePairReaders);
        int i = 0;
        while (seriesMergeSortReader.hasNext()) {
            TimeValuePair timeValuePair = seriesMergeSortReader.next();
            Assert.assertEquals(retTimestamp[i], timeValuePair.getTimestamp());
            Assert.assertEquals(retValue[i], timeValuePair.getValue().getValue());
            i++;
        }
    }

    public static class FakedSeriesReader implements SeriesReader {

        private long[] timestamps;
        private int index;
        private long value;

        public FakedSeriesReader(long[] timestamps) {
            this.timestamps = timestamps;
            index = 0;
            value = 1L;
        }

        public FakedSeriesReader(long[] timestamps, long value) {
            this.timestamps = timestamps;
            index = 0;
            this.value = value;
        }

        @Override
        public boolean hasNext() throws IOException {
            return index < timestamps.length;
        }

        @Override
        public TimeValuePair next() throws IOException {
            return new TimeValuePair(timestamps[index++], new TsPrimitiveType.TsLong(value));
        }

        @Override
        public void skipCurrentTimeValuePair() throws IOException {
            next();
        }

        @Override
        public void close() throws IOException {

        }
    }
}
