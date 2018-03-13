package cn.edu.tsinghua.iotdb.queryV2.reader;

import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityMergeSortTimeValuePairReader;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.PriorityTimeValuePairReader;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.series.OverflowInsertDataReader;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by zhangjinrui on 2018/1/22.
 */
public class OverflowInsertDataReaderTest {

    @Test
    public void testPeek() throws IOException {
        long[] ret1 = new long[]{1, 2, 3, 4, 5, 6, 7, 8};
        test(ret1);
        long[] ret2 = new long[]{1};
        test(ret2);
        long[] ret3 = new long[]{};
        test(ret3);
    }

    public void test(long[] ret) throws IOException {
        SeriesMergeSortReaderTest.FakedSeriesReader fakedSeriesReader = new SeriesMergeSortReaderTest.FakedSeriesReader(
                ret);
        OverflowInsertDataReader overflowInsertDataReader = new OverflowInsertDataReader(1L,
                new PriorityMergeSortTimeValuePairReader(new PriorityTimeValuePairReader(fakedSeriesReader, new PriorityTimeValuePairReader.Priority(1))));
        for (int i = 0; i < ret.length; i++) {
            for (int j = 0; j < 10; j++) {
                Assert.assertEquals(ret[i], overflowInsertDataReader.peek().getTimestamp());
            }
            Assert.assertEquals(ret[i], overflowInsertDataReader.next().getTimestamp());
        }
    }
}
