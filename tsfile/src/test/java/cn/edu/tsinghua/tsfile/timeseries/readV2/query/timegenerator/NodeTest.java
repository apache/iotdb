package cn.edu.tsinghua.tsfile.timeseries.readV2.query.timegenerator;

import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.timegenerator.node.AndNode;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.timegenerator.node.LeafNode;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.timegenerator.node.Node;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.timegenerator.node.OrNode;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by zhangjinrui on 2017/12/26.
 */
public class NodeTest {

    @Test
    public void testLeafNode() throws IOException {
        int index = 0;
        long[] timestamps = new long[]{1, 2, 3, 4, 5, 6, 7};
        SeriesReader seriesReader = new FakedSeriesReader(timestamps);
        Node leafNode = new LeafNode(seriesReader);
        while (leafNode.hasNext()) {
            Assert.assertEquals(timestamps[index++], leafNode.next());
        }
    }

    @Test
    public void testOrNode() throws IOException {
        long[] ret = new long[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20};
        long[] left = new long[]{1, 3, 5, 7, 9, 10, 20};
        long[] right = new long[]{2, 3, 4, 5, 6, 7, 8};
        testOr(ret, left, right);
        testOr(new long[]{}, new long[]{}, new long[]{});
        testOr(new long[]{1}, new long[]{1}, new long[]{});
        testOr(new long[]{1}, new long[]{1}, new long[]{1});
        testOr(new long[]{1, 2}, new long[]{1}, new long[]{1, 2});
        testOr(new long[]{1, 2}, new long[]{1, 2}, new long[]{1, 2});
        testOr(new long[]{1, 2, 3}, new long[]{1, 2}, new long[]{1, 2, 3});
    }

    private void testOr(long[] ret, long[] left, long[] right) throws IOException {
        int index = 0;
        Node orNode = new OrNode(new LeafNode(new FakedSeriesReader(left)),
                new LeafNode(new FakedSeriesReader(right)));
        while (orNode.hasNext()) {
            long value = orNode.next();
            Assert.assertEquals(ret[index++], value);
        }
        Assert.assertEquals(ret.length, index);
    }

    @Test
    public void testAndNode() throws IOException {
        testAnd(new long[]{}, new long[]{1, 2, 3, 4}, new long[]{});
        testAnd(new long[]{}, new long[]{1, 2, 3, 4, 8}, new long[]{5, 6, 7});
        testAnd(new long[]{2}, new long[]{1, 2, 3, 4}, new long[]{2, 5, 6});
        testAnd(new long[]{1, 2, 3}, new long[]{1, 2, 3, 4}, new long[]{1, 2, 3});
        testAnd(new long[]{1, 2, 3, 9}, new long[]{1, 2, 3, 4, 9}, new long[]{1, 2, 3, 8, 9});
    }

    private void testAnd(long[] ret, long[] left, long[] right) throws IOException {
        int index = 0;
        Node andNode = new AndNode(new LeafNode(new FakedSeriesReader(left)),
                new LeafNode(new FakedSeriesReader(right)));
        while (andNode.hasNext()) {
            long value = andNode.next();
            Assert.assertEquals(ret[index++], value);
        }
        Assert.assertEquals(ret.length, index);
    }


    private static class FakedSeriesReader implements SeriesReader {

        private long[] timestamps;
        private int index;

        public FakedSeriesReader(long[] timestamps) {
            this.timestamps = timestamps;
            index = 0;
        }

        @Override
        public boolean hasNext() throws IOException {
            return index < timestamps.length;
        }

        @Override
        public TimeValuePair next() throws IOException {
            return new TimeValuePair(timestamps[index++], new TsPrimitiveType.TsLong(1L));
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
