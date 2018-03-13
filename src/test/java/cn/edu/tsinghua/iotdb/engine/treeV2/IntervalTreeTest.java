package cn.edu.tsinghua.iotdb.engine.treeV2;

import cn.edu.tsinghua.iotdb.engine.overflow.treeV2.IntervalTree;
import cn.edu.tsinghua.iotdb.engine.overflow.utils.OverflowOpType;
import cn.edu.tsinghua.iotdb.engine.overflow.utils.TimePair;
import cn.edu.tsinghua.tsfile.common.utils.BytesUtils;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeriesType;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static cn.edu.tsinghua.tsfile.common.utils.ReadWriteStreamUtils.readUnsignedVarInt;


/**
 * @author CGF
 */
public class IntervalTreeTest {

    // private static final logger LOG = LoggerFactory.getLogger(IntervalTreeTest.class);

    private static final byte[] i0 = BytesUtils.intToBytes(0);
    private static final byte[] i1 = BytesUtils.intToBytes(1);
    private static final byte[] i2 = BytesUtils.intToBytes(2);
    private static final byte[] i3 = BytesUtils.intToBytes(3);
    private static final byte[] i4 = BytesUtils.intToBytes(4);

    private static final byte[] l0 = BytesUtils.longToBytes(0L);
    private static final byte[] l1 = BytesUtils.longToBytes(1L);
    private static final byte[] l2 = BytesUtils.longToBytes(2L);
    private static final byte[] l3 = BytesUtils.longToBytes(3L);
    private static final byte[] l4 = BytesUtils.longToBytes(4L);

    private static final byte[] f0 = BytesUtils.floatToBytes(0.7f);
    private static final byte[] f1 = BytesUtils.floatToBytes(1.7f);
    private static final byte[] f2 = BytesUtils.floatToBytes(2.7f);
    private static final byte[] f3 = BytesUtils.floatToBytes(3.7f);
    private static final byte[] f4 = BytesUtils.floatToBytes(4.7f);

    private static final byte[] d0 = BytesUtils.doubleToBytes(0.6);
    private static final byte[] d1 = BytesUtils.doubleToBytes(1.6);
    private static final byte[] d2 = BytesUtils.doubleToBytes(2.6);
    private static final byte[] d3 = BytesUtils.doubleToBytes(3.6);
    private static final byte[] d4 = BytesUtils.doubleToBytes(4.6);

    private static final byte[] s0 = BytesUtils.StringToBytes("tsfile0");
    private static final byte[] s1 = BytesUtils.StringToBytes("tsfile1");
    private static final byte[] s2 = BytesUtils.StringToBytes("tsfile2");
    private static final byte[] s3 = BytesUtils.StringToBytes("tsfile3");
    private static final byte[] s4 = BytesUtils.StringToBytes("tsfile4");

    private static double delta = 0.000001;

    @Test
    public void insertTest() {
        IntervalTree tree = new IntervalTree();

        TimePair i1 = new TimePair(100L, 100L, IntervalTreeTest.i1, OverflowOpType.INSERT);
        TimePair i2 = new TimePair(500L, 500L, IntervalTreeTest.i2, OverflowOpType.INSERT);
        tree.update(i1);
        tree.update(i2);
        TimePair queryTp = new TimePair(0L, 10000L);
        List<TimePair> ans = tree.query(queryTp);

        int count = 0;
        for (TimePair tp : ans) {
            if (count == 0) {
                Assert.assertEquals(tp.s, 100L);
                Assert.assertEquals(BytesUtils.bytesToInt(tp.v), 1);
            }
            if (count == 1) {
                Assert.assertEquals(tp.s, 500L);
                Assert.assertEquals(BytesUtils.bytesToInt(tp.v), 2);
            }
            count++;
        }
    }

    @Test
    public void updateTest() {
        IntervalTree tree = new IntervalTree();

        TimePair i1 = new TimePair(100L, 200L, IntervalTreeTest.i1, OverflowOpType.UPDATE);
        TimePair i2 = new TimePair(300L, 500L, IntervalTreeTest.i2, OverflowOpType.UPDATE);
        tree.update(i1);
        tree.update(i2);
        TimePair queryTp = new TimePair(0L, 10000L);
        List<TimePair> ans = tree.query(queryTp);

        int count = 0;
        for (TimePair tp : ans) {
            if (count == 0) {
                Assert.assertEquals(tp.s, 100L);
                Assert.assertEquals(tp.e, 200L);
                Assert.assertEquals(BytesUtils.bytesToInt(tp.v), 1);
            }
            if (count == 1) {
                Assert.assertEquals(tp.s, 300L);
                Assert.assertEquals(tp.e, 500L);
                Assert.assertEquals(BytesUtils.bytesToInt(tp.v), 2);
            }
            count++;
        }
    }

    /**
     * fix a bug.
     * t1. [0, 100] -> DELETE operation.
     * t2. [0, 500] -> UPDATE operation.
     * t2 covers t1.
     */
    @Test
    public void bugFixTest() {
        IntervalTree tree = new IntervalTree();
        TimePair d1 = new TimePair(0L, 100L, IntervalTreeTest.i1, OverflowOpType.DELETE);
        TimePair u1 = new TimePair(0L, 500L, IntervalTreeTest.i2, OverflowOpType.UPDATE);
        tree.update(d1);
        tree.update(u1);
        DynamicOneColumnData crudResult = tree.dynamicQuery(null,  TSDataType.INT32);
        for (int i = 0; i < crudResult.valueLength; i++) {
            if (i == 0) {
                Assert.assertEquals(crudResult.getTime(i * 2), 0);
                Assert.assertEquals(crudResult.getTime(i * 2 + 1), -100);
                Assert.assertEquals(crudResult.getInt(i), 0);
            } else if (i == 1) {
                Assert.assertEquals(crudResult.getTime(i * 2), 101);
                Assert.assertEquals(crudResult.getTime(i * 2 + 1), 500);
                Assert.assertEquals(crudResult.getInt(i), 2);
            }
        }

        // [0, 20] DELETE
        // [10, 30] UPDATE
        tree = new IntervalTree();
        TimePair d2 = new TimePair(0L, 20L, IntervalTreeTest.i1, OverflowOpType.DELETE);
        TimePair u2 = new TimePair(10L, 30L, IntervalTreeTest.i2, OverflowOpType.UPDATE);
        tree.update(d2);
        tree.update(u2);
        crudResult = tree.dynamicQuery(null,  TSDataType.INT32);
        for (int i = 0; i < crudResult.valueLength; i++) {
            if (i == 0) {
                Assert.assertEquals(crudResult.getTime(i * 2), 0);
                Assert.assertEquals(crudResult.getTime(i * 2 + 1), -20);
                Assert.assertEquals(crudResult.getInt(i), 0);
            } else if (i == 1) {
                Assert.assertEquals(crudResult.getTime(i * 2), 21);
                Assert.assertEquals(crudResult.getTime(i * 2 + 1), 30);
                Assert.assertEquals(crudResult.getInt(i), 2);
            }
        }

        // [0, 20] DELETE
        // [0, 20] UPDATE
        tree = new IntervalTree();
        d2 = new TimePair(0L, 20L, IntervalTreeTest.i1, OverflowOpType.DELETE);
        u2 = new TimePair(0L, 20L, IntervalTreeTest.i2, OverflowOpType.UPDATE);
        tree.update(d2);
        tree.update(u2);
        crudResult = tree.dynamicQuery(null,  TSDataType.INT32);
        Assert.assertEquals(crudResult.valueLength, 1);
        for (int i = 0; i < crudResult.valueLength; i++) {
            if (i == 0) {
                Assert.assertEquals(crudResult.getTime(i * 2), 0);
                Assert.assertEquals(crudResult.getTime(i * 2 + 1), -20);
                Assert.assertEquals(crudResult.getInt(i), 0);
            }
        }

        // [0, 20] DELETE
        // [0, 10] UPDATE
        tree = new IntervalTree();
        d2 = new TimePair(0L, 20L, IntervalTreeTest.i1, OverflowOpType.DELETE);
        u2 = new TimePair(0L, 10L, IntervalTreeTest.i2, OverflowOpType.UPDATE);
        tree.update(d2);
        tree.update(u2);
        crudResult = tree.dynamicQuery(null,  TSDataType.INT32);
        Assert.assertEquals(crudResult.valueLength, 1);
        for (int i = 0; i < crudResult.valueLength; i++) {
            if (i == 0) {
                Assert.assertEquals(crudResult.getTime(i * 2), 0);
                Assert.assertEquals(crudResult.getTime(i * 2 + 1), -20);
                Assert.assertEquals(crudResult.getInt(i), 0);
            }
        }

        // [0, 20] DELETE
        // [10, 20] UPDATE
        tree = new IntervalTree();
        d2 = new TimePair(0L, 20L, IntervalTreeTest.i1, OverflowOpType.DELETE);
        u2 = new TimePair(10L, 10L, IntervalTreeTest.i2, OverflowOpType.UPDATE);
        tree.update(d2);
        tree.update(u2);
        crudResult = tree.dynamicQuery(null,  TSDataType.INT32);
        Assert.assertEquals(crudResult.valueLength, 1);
        for (int i = 0; i < crudResult.valueLength; i++) {
            if (i == 0) {
                Assert.assertEquals(crudResult.getTime(i * 2), 0);
                Assert.assertEquals(crudResult.getTime(i * 2 + 1), -20);
                Assert.assertEquals(crudResult.getInt(i), 0);
            }
        }

        // [0, 40] DELETE
        // [10, 20] UPDATE
        tree = new IntervalTree();
        d2 = new TimePair(0L, 40L, IntervalTreeTest.i1, OverflowOpType.DELETE);
        u2 = new TimePair(10L, 20L, IntervalTreeTest.i2, OverflowOpType.UPDATE);
        tree.update(d2);
        tree.update(u2);
        crudResult = tree.dynamicQuery(null,  TSDataType.INT32);
        Assert.assertEquals(crudResult.valueLength, 1);
        for (int i = 0; i < crudResult.valueLength; i++) {
            if (i == 0) {
                Assert.assertEquals(crudResult.getTime(i * 2), 0);
                Assert.assertEquals(crudResult.getTime(i * 2 + 1), -40);
                Assert.assertEquals(crudResult.getInt(i), 0);
            }
        }

        // [5, 5] INSERT
        // [1, 10] UPDATE
        tree = new IntervalTree();
        TimePair i1 = new TimePair(5L, 5L, IntervalTreeTest.i1, OverflowOpType.INSERT);
        u2 = new TimePair(1L, 10L, IntervalTreeTest.i2, OverflowOpType.UPDATE);
        tree.update(i1);
        tree.update(u2);
        crudResult = tree.dynamicQuery(null,  TSDataType.INT32);
        Assert.assertEquals(crudResult.valueLength, 2);
        for (int i = 0; i < crudResult.valueLength; i++) {
            if (i == 0) {
                Assert.assertEquals(crudResult.getTime(i * 2), 1);
                Assert.assertEquals(crudResult.getTime(i * 2 + 1), 4);
                Assert.assertEquals(crudResult.getInt(i), 2);
            } else if (i == 2) {
                Assert.assertEquals(crudResult.getTime(i * 2), 6);
                Assert.assertEquals(crudResult.getTime(i * 2 + 1), 10);
                Assert.assertEquals(crudResult.getInt(i), 2);
            }
        }

        // [0, 5] DELETE
        // [7, 10] UPDATE
        tree = new IntervalTree();
        d1 = new TimePair(0L, 5L, IntervalTreeTest.i1, OverflowOpType.DELETE);
        u2 = new TimePair(7L, 10L, IntervalTreeTest.i2, OverflowOpType.UPDATE);
        tree.update(d1);
        tree.update(u2);
        SingleSeriesFilterExpression filter = FilterFactory.gtEq(FilterFactory.longFilterSeries(
                "Any", "Any", FilterSeriesType.TIME_FILTER), 7L, true);
        crudResult = tree.dynamicQuery(filter,  TSDataType.INT32);
        Assert.assertEquals(crudResult.valueLength, 1);
        for (int i = 0; i < crudResult.valueLength; i++) {
            if (i == 0) {
                Assert.assertEquals(crudResult.getTime(i * 2), 7);
                Assert.assertEquals(crudResult.getTime(i * 2 + 1), 10);
                Assert.assertEquals(crudResult.getInt(i), 2);
            }
        }

        // [5, 5] INSERT
        // [5, 5] UPDATE
        tree = new IntervalTree();
        i1 = new TimePair(5L, 5L, IntervalTreeTest.i1, OverflowOpType.INSERT);
        u2 = new TimePair(5L, 5L, IntervalTreeTest.i2, OverflowOpType.UPDATE);
        tree.update(i1);
        tree.update(u2);
        crudResult = tree.dynamicQuery(null,  TSDataType.INT32);
        Assert.assertEquals(crudResult.valueLength, 0);

        // [5, 5] UPDATE
        // [5, 5] UPDATE
        tree = new IntervalTree();
        u1 = new TimePair(5L, 5L, IntervalTreeTest.i1, OverflowOpType.UPDATE);
        u2 = new TimePair(5L, 5L, IntervalTreeTest.i2, OverflowOpType.UPDATE);
        tree.update(u1);
        tree.update(u2);
        crudResult = tree.dynamicQuery(null,  TSDataType.INT32);
        Assert.assertEquals(crudResult.valueLength, 1);
        for (int i = 0; i < crudResult.valueLength; i++) {
            if (i == 0) {
                Assert.assertEquals(crudResult.getTime(i * 2), 5);
                Assert.assertEquals(crudResult.getTime(i * 2 + 1), 5);
                Assert.assertEquals(crudResult.getInt(i), 2);
            }
        }

        // [5, 10] UPDATE
        // [5, 7] UPDATE
        tree = new IntervalTree();
        u1 = new TimePair(5L, 10L, IntervalTreeTest.i1, OverflowOpType.UPDATE);
        u2 = new TimePair(5L, 6L, IntervalTreeTest.i2, OverflowOpType.UPDATE);
        tree.update(u1);
        tree.update(u2);
        filter = FilterFactory.ltEq(FilterFactory.longFilterSeries(
                "Any", "Any", FilterSeriesType.TIME_FILTER), 4L, true);
        crudResult = tree.dynamicQuery(filter,  TSDataType.INT32);
        Assert.assertEquals(crudResult.valueLength, 0);

        filter = FilterFactory.ltEq(FilterFactory.longFilterSeries(
                "Any", "Any", FilterSeriesType.TIME_FILTER), 5L, true);
        crudResult = tree.dynamicQuery(filter,  TSDataType.INT32);
        Assert.assertEquals(crudResult.valueLength, 1);
        for (int i = 0; i < crudResult.valueLength; i++) {
            if (i == 0) {
                Assert.assertEquals(crudResult.getTime(i * 2), 5);
                Assert.assertEquals(crudResult.getTime(i * 2 + 1), 5);
                Assert.assertEquals(crudResult.getInt(i), 2);
            }
        }

        // [5, 7] DELETE
        // [6, 7] UPDATE
        tree = new IntervalTree();
        d1 = new TimePair(5L, 7L, IntervalTreeTest.i1, OverflowOpType.DELETE);
        u2 = new TimePair(6L, 6L, IntervalTreeTest.i2, OverflowOpType.UPDATE);
        tree.update(d1);
        tree.update(u2);
        filter = FilterFactory.ltEq(FilterFactory.longFilterSeries(
                "Any", "Any", FilterSeriesType.TIME_FILTER), 6L, true);
        SingleSeriesFilterExpression filter2 = FilterFactory.gtEq(FilterFactory.longFilterSeries(
                "Any", "Any", FilterSeriesType.TIME_FILTER), 6L, true);
        SingleSeriesFilterExpression filterAnd = (SingleSeriesFilterExpression) FilterFactory.and(filter, filter2);
        crudResult = tree.dynamicQuery(filterAnd,  TSDataType.INT32);
        Assert.assertEquals(crudResult.valueLength, 1);
        for (int i = 0; i < crudResult.valueLength; i++) {
            if (i == 0) {
                Assert.assertEquals(crudResult.getTime(i * 2), -6);
                Assert.assertEquals(crudResult.getTime(i * 2 + 1), -6);
            }
        }
        filter = FilterFactory.gtEq(FilterFactory.longFilterSeries(
                "Any", "Any", FilterSeriesType.TIME_FILTER), 6L, true);
        crudResult = tree.dynamicQuery(filter,  TSDataType.INT32);
        Assert.assertEquals(crudResult.valueLength, 1);
        for (int i = 0; i < crudResult.valueLength; i++) {
            if (i == 0) {
                Assert.assertEquals(crudResult.getTime(i * 2), -6);
                Assert.assertEquals(crudResult.getTime(i * 2 + 1), -7);
            }
        }

        // [5, 7] DELETE
        // [6, 7] UPDATE
        tree = new IntervalTree();
        d1 = new TimePair(5L, 7L, IntervalTreeTest.i1, OverflowOpType.DELETE);
        u2 = new TimePair(6L, 7L, IntervalTreeTest.i2, OverflowOpType.UPDATE);
        tree.update(d1);
        tree.update(u2);
        crudResult = tree.dynamicQuery(null,  TSDataType.INT32);
        Assert.assertEquals(crudResult.valueLength, 1);
        for (int i = 0; i < crudResult.valueLength; i++) {
            if (i == 0) {
                Assert.assertEquals(crudResult.getTime(i * 2), -5);
                Assert.assertEquals(crudResult.getTime(i * 2 + 1), -7);
            }
        }

        // [5, 7] UPDATE
        // [6, 7] UPDATE
        tree = new IntervalTree();
        u1 = new TimePair(5L, 7L, IntervalTreeTest.i1, OverflowOpType.UPDATE);
        u2 = new TimePair(6L, 7L, IntervalTreeTest.i2, OverflowOpType.UPDATE);
        tree.update(u1);
        tree.update(u2);
        crudResult = tree.dynamicQuery(null,  TSDataType.INT32);
        Assert.assertEquals(crudResult.valueLength, 2);
        for (int i = 0; i < crudResult.valueLength; i++) {
            if (i == 0) {
                Assert.assertEquals(crudResult.getTime(i * 2), 5);
                Assert.assertEquals(crudResult.getTime(i * 2 + 1), 5);
                Assert.assertEquals(crudResult.getInt(i), 1);
            } else if (i == 1) {
                Assert.assertEquals(crudResult.getTime(i * 2), 6);
                Assert.assertEquals(crudResult.getTime(i * 2 + 1), 7);
                Assert.assertEquals(crudResult.getInt(i), 2);

            }
        }

        // [5, 7] UPDATE
        // [6, 9] UPDATE
        tree = new IntervalTree();
        u1 = new TimePair(5L, 7L, IntervalTreeTest.i1, OverflowOpType.UPDATE);
        u2 = new TimePair(6L, 9L, IntervalTreeTest.i2, OverflowOpType.UPDATE);
        tree.update(u1);
        tree.update(u2);
        crudResult = tree.dynamicQuery(null,  TSDataType.INT32);
        Assert.assertEquals(crudResult.valueLength, 2);
        for (int i = 0; i < crudResult.valueLength; i++) {
            if (i == 0) {
                Assert.assertEquals(crudResult.getTime(i * 2), 5);
                Assert.assertEquals(crudResult.getTime(i * 2 + 1), 5);
                Assert.assertEquals(crudResult.getInt(i), 1);
            } else if (i == 1) {
                Assert.assertEquals(crudResult.getTime(i * 2), 6);
                Assert.assertEquals(crudResult.getTime(i * 2 + 1), 9);
                Assert.assertEquals(crudResult.getInt(i), 2);

            }
        }
    }

    /**
     * -0 -250 0
     * 251 490 3
     * 500 -500 2
     */
    @Test
    public void intDataMixTest() {
        IntervalTree tree = new IntervalTree();

        TimePair i1 = new TimePair(100L, 100L, IntervalTreeTest.i1, OverflowOpType.INSERT);
        TimePair i2 = new TimePair(500L, 500L, IntervalTreeTest.i2, OverflowOpType.INSERT);
        TimePair u1 = new TimePair(250L, 490L, i3, OverflowOpType.UPDATE);
        TimePair d1 = new TimePair(0L, 250L, i0, OverflowOpType.DELETE);
        TimePair u2 = new TimePair(130L, 140L, i4, OverflowOpType.UPDATE);
        tree.update(i1);
        tree.update(i2);
        tree.update(u1);
        tree.update(d1);
        tree.update(u2);

        DynamicOneColumnData crudResult = tree.dynamicQuery(null,  TSDataType.INT32);

        Assert.assertEquals(crudResult.valueLength, 2);
        for (int i = 0; i < crudResult.valueLength; i++) {
            if (i == 0) {
                Assert.assertEquals(crudResult.getTime(i * 2), 0);
                Assert.assertEquals(crudResult.getTime(i * 2 + 1), -250);
                Assert.assertEquals(crudResult.getInt(i), 0);
            } else if (i == 1) {
                Assert.assertEquals(crudResult.getTime(i * 2), 251);
                Assert.assertEquals(crudResult.getTime(i * 2 + 1), 490);
                Assert.assertEquals(crudResult.getInt(i), 3);
            }
        }
    }

    /**
     * -0 -250 0
     * 251 490 3.7
     * 500 -500 2.7
     */
    @Test
    public void floatDataMixTest() {
        IntervalTree tree = new IntervalTree();

        TimePair i1 = new TimePair(100L, 100L, IntervalTreeTest.f1, OverflowOpType.INSERT);
        TimePair i2 = new TimePair(500L, 500L, IntervalTreeTest.f2, OverflowOpType.INSERT);
        TimePair u1 = new TimePair(250L, 490L, f3, OverflowOpType.UPDATE);
        TimePair d1 = new TimePair(0L, 250L, f0, OverflowOpType.DELETE);
        TimePair u2 = new TimePair(130L, 140L, f4, OverflowOpType.UPDATE);
        tree.update(i1);
        tree.update(i2);
        tree.update(u1);
        tree.update(d1);
        tree.update(u2);

        DynamicOneColumnData crudResult = tree.dynamicQuery(null,  TSDataType.FLOAT);

        Assert.assertEquals(crudResult.valueLength, 2);
        for (int i = 0; i < crudResult.valueLength; i++) {
            if (i == 0) {
                Assert.assertEquals(crudResult.getTime(i * 2), 0);
                Assert.assertEquals(crudResult.getTime(i * 2 + 1), -250);
                Assert.assertEquals(crudResult.getFloat(i), 0, delta);
            } else if (i == 1) {
                Assert.assertEquals(crudResult.getTime(i * 2), 251);
                Assert.assertEquals(crudResult.getTime(i * 2 + 1), 490);
                Assert.assertEquals(crudResult.getFloat(i), 3.7, delta);
            }
        }
    }

    /**
     * 0 -250 0
     * 251 490 3L
     * 500 -500 2L
     */
    @Test
    public void longDataMixTest() {
        IntervalTree tree = new IntervalTree();

        TimePair i1 = new TimePair(100L, 100L, IntervalTreeTest.l1, OverflowOpType.INSERT);
        TimePair i2 = new TimePair(500L, 500L, IntervalTreeTest.l2, OverflowOpType.INSERT);
        TimePair u1 = new TimePair(250L, 490L, l3, OverflowOpType.UPDATE);
        TimePair d1 = new TimePair(0L, 250L, l0, OverflowOpType.DELETE);
        TimePair u2 = new TimePair(130L, 140L, l4, OverflowOpType.UPDATE);
        tree.update(i1);
        tree.update(i2);
        tree.update(u1);
        tree.update(d1);
        tree.update(u2);

        DynamicOneColumnData crudResult = tree.dynamicQuery(null,  TSDataType.INT64);

        Assert.assertEquals(crudResult.valueLength, 2);
        for (int i = 0; i < crudResult.valueLength; i++) {
            if (i == 0) {
                Assert.assertEquals(crudResult.getTime(i * 2), 0);
                Assert.assertEquals(crudResult.getTime(i * 2 + 1), -250);
                Assert.assertEquals(crudResult.getLong(i), 0);
            } else if (i == 1) {
                Assert.assertEquals(crudResult.getTime(i * 2), 251);
                Assert.assertEquals(crudResult.getTime(i * 2 + 1), 490);
                Assert.assertEquals(crudResult.getLong(i), 3L);
            }
        }
    }

    /**
     * -0  -250 0
     * 251 490 3.6
     * 500 -500 2.6
     */
    @Test
    public void doubleDataMixTest() {
        IntervalTree tree = new IntervalTree();

        TimePair i1 = new TimePair(100L, 100L, IntervalTreeTest.d1, OverflowOpType.INSERT);
        TimePair i2 = new TimePair(500L, 500L, IntervalTreeTest.d2, OverflowOpType.INSERT);
        TimePair u1 = new TimePair(250L, 490L, d3, OverflowOpType.UPDATE);
        TimePair d1 = new TimePair(0L, 250L, d0, OverflowOpType.DELETE);
        TimePair u2 = new TimePair(130L, 140L, d4, OverflowOpType.UPDATE);
        tree.update(i1);
        tree.update(i2);
        tree.update(u1);
        tree.update(d1);
        tree.update(u2);

        DynamicOneColumnData crudResult = tree.dynamicQuery(null,  TSDataType.DOUBLE);

        Assert.assertEquals(crudResult.valueLength, 2);
        for (int i = 0; i < crudResult.valueLength; i++) {
            if (i == 0) {
                Assert.assertEquals(crudResult.getTime(i * 2), 0);
                Assert.assertEquals(crudResult.getTime(i * 2 + 1), -250);
                Assert.assertEquals(crudResult.getDouble(i), 0, delta);
            } else if (i == 1) {
                Assert.assertEquals(crudResult.getTime(i * 2), 251);
                Assert.assertEquals(crudResult.getTime(i * 2 + 1), 490);
                Assert.assertEquals(crudResult.getDouble(i), 3.6, delta);
            }
        }
    }

    /**
     * -0 -250 tsfile0
     * 251 490 tsfile3
     * 500 -500 tfile2
     */
    @Test
    public void stringDataMixTest() {
        IntervalTree tree = new IntervalTree(TSDataType.TEXT);

        TimePair i1 = new TimePair(100L, 100L, IntervalTreeTest.s1, OverflowOpType.INSERT, TSDataType.TEXT);
        TimePair i2 = new TimePair(500L, 500L, IntervalTreeTest.s2, OverflowOpType.INSERT, TSDataType.TEXT);
        TimePair u1 = new TimePair(250L, 490L, s3, OverflowOpType.UPDATE, TSDataType.TEXT);
        TimePair d1 = new TimePair(0L, 250L, s0, OverflowOpType.DELETE, TSDataType.TEXT);
        TimePair u2 = new TimePair(130L, 140L, s4, OverflowOpType.UPDATE, TSDataType.TEXT);
        tree.update(i1);
        tree.update(i2);
        tree.update(u1);
        tree.update(d1);
        tree.update(u2);

        DynamicOneColumnData crudResult = tree.dynamicQuery(null,  TSDataType.TEXT);

        Assert.assertEquals(crudResult.valueLength, 2);
        for (int i = 0; i < crudResult.valueLength; i++) {
            if (i == 0) {
                Assert.assertEquals(crudResult.getTime(i * 2), 0);
                Assert.assertEquals(crudResult.getTime(i * 2 + 1), -250);
                Assert.assertEquals(crudResult.getBinary(i).getStringValue(), "");
            } else if (i == 1) {
                Assert.assertEquals(crudResult.getTime(i * 2), 251);
                Assert.assertEquals(crudResult.getTime(i * 2 + 1), 490);
                Assert.assertEquals(crudResult.getBinary(i).getStringValue(), "tsfile3");
            }
        }
    }

    /**
     * -0 -250 tsfile0
     * 251 490 tsfile3
     * 500 -500 tfile2
     */
    @Test
    public void stringSerializationTest() throws IOException {
        IntervalTree tree = new IntervalTree(TSDataType.TEXT);

        TimePair i1 = new TimePair(100L, 100L, IntervalTreeTest.s1, OverflowOpType.INSERT, TSDataType.TEXT);
        TimePair i2 = new TimePair(500L, 500L, IntervalTreeTest.s2, OverflowOpType.INSERT, TSDataType.TEXT);
        TimePair u1 = new TimePair(250L, 490L, s3, OverflowOpType.UPDATE, TSDataType.TEXT);
        TimePair d1 = new TimePair(0L, 250L, s0, OverflowOpType.DELETE, TSDataType.TEXT);
        TimePair u2 = new TimePair(130L, 140L, s4, OverflowOpType.UPDATE, TSDataType.TEXT);
        tree.update(i1);
        tree.update(i2);
        tree.update(u1);
        tree.update(d1);
        tree.update(u2);

        ByteArrayOutputStream outStream  = new ByteArrayOutputStream();
        tree.midOrderSerialize(outStream);

        InputStream inputStream = new ByteArrayInputStream(outStream.toByteArray());
        while (inputStream.available() != 0) {
            long s = BytesUtils.readLong(inputStream);
            long e = BytesUtils.readLong(inputStream);
            if (s <= 0 && e <= 0) {
                continue;
            }
            int len = readUnsignedVarInt(inputStream);
            byte[] stringBytes = new byte[len];
            inputStream.read(stringBytes);
            if (s == 251 && e == 490) {
                Assert.assertEquals(BytesUtils.bytesToString(stringBytes), "tsfile3");
            }
        }
    }
}