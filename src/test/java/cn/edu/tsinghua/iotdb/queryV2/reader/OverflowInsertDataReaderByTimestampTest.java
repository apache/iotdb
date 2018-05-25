package cn.edu.tsinghua.iotdb.queryV2.reader;

import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityMergeSortTimeValuePairReader;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityMergeSortTimeValuePairReaderByTimestamp;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityTimeValuePairReader;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityTimeValuePairReaderByTimestamp;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.series.OverflowInsertDataReader;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.series.OverflowInsertDataReaderByTimeStamp;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReaderByTimeStamp;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class OverflowInsertDataReaderByTimestampTest {
    @Test
    public void testPeek() throws IOException {
        long[] ret1 = new long[]{1, 3, 5, 7, 9, 10, 17, 18};
        test(ret1);
        long[] ret2 = new long[]{1};
        test(ret2);
        long[] ret3 = new long[]{};
        test(ret3);
    }

    public void test(long[] ret) throws IOException {
        FakedSeriesReaderByTimestamp fakedSeriesReader = new FakedSeriesReaderByTimestamp(ret);
        OverflowInsertDataReaderByTimeStamp overflowInsertDataReaderByTimeStamp = new OverflowInsertDataReaderByTimeStamp(1L,
                new PriorityMergeSortTimeValuePairReaderByTimestamp
                (new PriorityTimeValuePairReaderByTimestamp(fakedSeriesReader, new PriorityTimeValuePairReader.Priority(1))));
        for (int i = 0; i < ret.length; i++) {
            overflowInsertDataReaderByTimeStamp.setCurrentTimestamp(2*i-1);

            if(overflowInsertDataReaderByTimeStamp.hasNext()){
                Assert.assertEquals(2*i-1, overflowInsertDataReaderByTimeStamp.next().getTimestamp());
            }
        }
    }

    public static class FakedSeriesReaderByTimestamp implements SeriesReaderByTimeStamp {

        private long[] timestamps;
        private int index;
        private long value;
        private long currentTimeStamp;

        public FakedSeriesReaderByTimestamp(long[] timestamps) {
            this.timestamps = timestamps;
            index = 0;
            value = 1L;
        }

        public FakedSeriesReaderByTimestamp(long[] timestamps, long value) {
            this.timestamps = timestamps;
            index = 0;
            this.value = value;
        }

        @Override
        public boolean hasNext() throws IOException {
            while (index < timestamps.length && timestamps[index] <= currentTimeStamp) {
                if (timestamps[index] == currentTimeStamp) {
                    return true;
                } else {
                    index++;
                }
            }
            return false;
        }

        @Override
        public TimeValuePair next() throws IOException {
            return new TimeValuePair(timestamps[index++], new TsPrimitiveType.TsLong(timestamps[index-1]));
        }

        @Override
        public void skipCurrentTimeValuePair() throws IOException {
            next();
        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public void setCurrentTimestamp(long timestamp) {
            currentTimeStamp = timestamp;
        }
    }
}