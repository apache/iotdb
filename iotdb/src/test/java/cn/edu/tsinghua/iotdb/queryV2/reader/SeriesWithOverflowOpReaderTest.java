package cn.edu.tsinghua.iotdb.queryV2.reader;

import cn.edu.tsinghua.iotdb.queryV2.engine.overflow.OverflowDeleteOperation;
import cn.edu.tsinghua.iotdb.queryV2.engine.overflow.OverflowOperation;
import cn.edu.tsinghua.iotdb.queryV2.engine.overflow.OverflowOperationReader;
import cn.edu.tsinghua.iotdb.queryV2.engine.overflow.OverflowUpdateOperation;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.series.SeriesWithOverflowOpReader;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.series.SeriesWithUpdateOpReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhangjinrui on 2018/1/19.
 */
public class SeriesWithOverflowOpReaderTest {

    @Test
    public void test1() throws IOException {
        OverflowUpdateOperation[] updateOps = new OverflowUpdateOperation[]{
                new OverflowUpdateOperation(0L, 100L, new TsPrimitiveType.TsInt(100)),
                new OverflowUpdateOperation(150L, 200L, new TsPrimitiveType.TsInt(200)),
                new OverflowUpdateOperation(300L, 400L, new TsPrimitiveType.TsInt(300))
        };
        testRead(genSeriesReader(1000), updateOps);
    }

    @Test
    public void test2() throws IOException {
        OverflowUpdateOperation[] updateOps = new OverflowUpdateOperation[]{
                new OverflowUpdateOperation(0L, 1L, new TsPrimitiveType.TsInt(100)),
        };
        testRead(genSeriesReader(1000), updateOps);
    }

    @Test
    public void test3() throws IOException {
        OverflowUpdateOperation[] updateOps = new OverflowUpdateOperation[]{
                new OverflowUpdateOperation(10000L, 20000L, new TsPrimitiveType.TsInt(100)),
        };
        testRead(genSeriesReader(1000), updateOps);
    }

    @Test
    public void test4() throws IOException {
        OverflowUpdateOperation[] updateOps = new OverflowUpdateOperation[]{
                new OverflowUpdateOperation(-1L, 0L, new TsPrimitiveType.TsInt(100)),
        };
        testRead(genSeriesReader(1000), updateOps);
    }

    private SeriesMergeSortReaderTest.FakedSeriesReader genSeriesReader(int size) {
        int count = size;
        long[] timestamps = new long[count];
        for (int i = 0; i < count; i++) {
            timestamps[i] = i;
        }
        return new SeriesMergeSortReaderTest.FakedSeriesReader(timestamps);
    }

    @Test
    public void testWithDelete1() throws IOException {
        OverflowOperation[] updateOps = new OverflowOperation[]{
                new OverflowUpdateOperation(0L, 100L, new TsPrimitiveType.TsInt(100)),
                new OverflowUpdateOperation(150L, 200L, new TsPrimitiveType.TsInt(200)),
                new OverflowDeleteOperation(300L, 400L)
        };
        testUpdateWithDelete(genSeriesReader(1000), updateOps);
    }

    @Test
    public void testWithDelete2() throws IOException {
        OverflowOperation[] updateOps = new OverflowOperation[]{
                new OverflowDeleteOperation(0L, 1000L)
        };
        testUpdateWithDelete(genSeriesReader(1000), updateOps);
    }

    @Test
    public void testWithDelete3() throws IOException {
        OverflowOperation[] updateOps = new OverflowOperation[]{
                new OverflowDeleteOperation(0L, 500L)
        };
        testUpdateWithDelete(genSeriesReader(1000), updateOps);
    }

    @Test
    public void testWithDelete4() throws IOException {
        OverflowOperation[] updateOps = new OverflowOperation[]{
                new OverflowUpdateOperation(0L, 100L, new TsPrimitiveType.TsInt(100)),
                new OverflowDeleteOperation(150L, 200L),
                new OverflowDeleteOperation(300L, 400L),
                new OverflowUpdateOperation(500L, 600L, new TsPrimitiveType.TsInt(100))
        };
        testUpdateWithDelete(genSeriesReader(1000), updateOps);
    }

    private void testUpdateWithDelete(SeriesReader seriesReader, OverflowOperation[] updateOperations) throws IOException {
        FakedOverflowOperationReader updateOpReader = new FakedOverflowOperationReader(updateOperations);
        SeriesWithOverflowOpReader seriesWithUpdateOpReader = new SeriesWithOverflowOpReader(seriesReader, updateOpReader);
        check(updateOpReader, seriesWithUpdateOpReader);
    }

    private void testRead(SeriesReader seriesReader, OverflowOperation[] updateOperations) throws IOException {
        FakedOverflowOperationReader updateOpReader = new FakedOverflowOperationReader(updateOperations);
        SeriesWithUpdateOpReader seriesWithUpdateOpReader = new SeriesWithUpdateOpReader(seriesReader, updateOpReader);
        check(updateOpReader, seriesWithUpdateOpReader);
    }

    private void check(FakedOverflowOperationReader opReader, SeriesReader seriesWithOverflowOpReader) throws IOException {
        opReader.reset();
        List<OverflowOperation> overflowOperations = new ArrayList<>();
        while (opReader.hasNext()) {
            overflowOperations.add(opReader.next());
        }
        opReader.reset();
        while (seriesWithOverflowOpReader.hasNext()) {
            TimeValuePair timeValuePair = seriesWithOverflowOpReader.next();
            boolean satisfied = false;
            for (int i = 0; i < overflowOperations.size(); i++) {
                if (satisfied(overflowOperations.get(i), timeValuePair.getTimestamp())) {
                    if (overflowOperations.get(i).getType() == OverflowOperation.OperationType.DELETE) {
                        Assert.fail("Deleted data has been read");
                    } else {
                        Assert.assertEquals(overflowOperations.get(i).getValue(), timeValuePair.getValue());
                        satisfied = true;
                    }
                }
            }
            if (!satisfied) {
                Assert.assertEquals(new TsPrimitiveType.TsLong(1), timeValuePair.getValue());
            }
        }
    }

    private boolean satisfied(OverflowOperation op, long timestamp) {
        if (op.getLeftBound() <= timestamp && timestamp <= op.getRightBound()) {
            return true;
        }
        return false;
    }

    public static class FakedOverflowOperationReader implements OverflowOperationReader {

        private OverflowOperation[] updataOps;
        private int index = 0;

        public FakedOverflowOperationReader(OverflowOperation[] updateOps) {
            this.updataOps = updateOps;
        }

        @Override
        public boolean hasNext() {
            return index < updataOps.length;
        }

        @Override
        public OverflowOperation next() {
            index++;
            return updataOps[index - 1];
        }

        @Override
        public OverflowOperation getCurrentOperation() {
            return updataOps[index];
        }

        @Override
        public void close() throws IOException {

        }

        @Override
        public OverflowOperationReader copy() {
            return null;
        }

        public void reset() {
            index = 0;
        }
    }
}
