package cn.edu.tsinghua.iotdb.engine.overflow;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import cn.edu.tsinghua.tsfile.common.utils.BytesUtils;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeriesType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.operators.GtEq;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;

/**
 * @author CGF.
 */
public class IntervalTreeOperationFileBlockTest {
    private static byte[] i1 = BytesUtils.intToBytes(1);
    private static byte[] i2 = BytesUtils.intToBytes(2);
    private static byte[] i3 = BytesUtils.intToBytes(3);

    private static byte[] l1 = BytesUtils.longToBytes(1L);
    private static byte[] l2 = BytesUtils.longToBytes(2L);

    private static byte[] f1 = BytesUtils.floatToBytes(1.0f);
    private static byte[] f2 = BytesUtils.floatToBytes(2.0f);

    private static byte[] d1 = BytesUtils.doubleToBytes(1.0);
    private static byte[] d2 = BytesUtils.doubleToBytes(2.0);

    private static byte[] s1 = BytesUtils.StringToBytes("s1");
    private static byte[] s2 = BytesUtils.StringToBytes("s2");

    private static double delta = 0.000001;

    // private static final logger LOG = LoggerFactory.getLogger(IntervalTreeOperationFileBlockTest.class);

    @Test
    public void intMixTest() throws IOException {
        IntervalTreeOperation tree = new IntervalTreeOperation(TSDataType.INT32);
        byte[] b1 = BytesUtils.intToBytes(100);
        byte[] b2 = BytesUtils.intToBytes(200);
        byte[] b3 = BytesUtils.intToBytes(300);
        byte[] b4 = BytesUtils.intToBytes(400);
        byte[] b5 = BytesUtils.intToBytes(500);

        ByteArrayInputStream[] ins = new ByteArrayInputStream[10];
        //  (1)
        tree.insert(1463369845000L, b1);
        tree.update(1463369845005L, 1463369845010L, b1);
        tree.update(1463369845015L, 1463369845025L, b1);
        tree.update(1463369845030L, 1463369845055L, b1);
        tree.update(1463369845070L, 1463369845080L, b1);
        tree.update(1463369845083L, 1463369845090L, b1);
        tree.insert(1463369845095L, b1);
        tree.insert(1463369845105L, b1);
        tree.insert(1463369845110L, b1);

        ByteArrayOutputStream out1 = new ByteArrayOutputStream();
        tree.toBytes(out1);

        byte[] page = out1.toByteArray();
        ins[1] = new ByteArrayInputStream(page);

        // (2)
        tree.update(1463369845100L, 1463369845110L, b2);
        ByteArrayOutputStream out2 = new ByteArrayOutputStream();
        tree.toBytes(out2);
        page = out2.toByteArray();
        ins[2] = new ByteArrayInputStream(page);

        // (3)
        tree.delete(1463369845044L);
        tree.update(1463369845060L, 1463369845070L, b3);
        tree.update(1463369845095L, 1463369845110L, b3);
        ByteArrayOutputStream out3 = new ByteArrayOutputStream();
        tree.toBytes(out3);
        page = out3.toByteArray();
        ins[3] = new ByteArrayInputStream(page);

        // (4)
        tree.update(1463369845035L, 1463369845050L, b4);
        ByteArrayOutputStream out4 = new ByteArrayOutputStream();
        tree.toBytes(out4);
        page = out4.toByteArray();
        ins[4] = new ByteArrayInputStream(page);

        // (5)
        tree.delete(1463369845019L);
        tree.insert(1463369845030L, b5);
        tree.insert(1463369845040L, b5);
        tree.update(1463369845060L, 1463369845065L, b5);
        tree.update(1463369845075L, 1463369845085L, b5);
        tree.insert(1463369845095L, b5);

        //  calc
        DynamicOneColumnData d = tree.queryMemory(null);

        for (int i = 4; i >= 1; i--) {
            d = tree.queryFileBlock(null, null, null, ins[i], d);
        }

        Assert.assertEquals(d.valueLength, 18);
        Assert.assertEquals(d.getTime(0), 0L);
        Assert.assertEquals(d.getTime(1), -1463369845019L);
        Assert.assertEquals(d.getInt(0), 0);

        Assert.assertEquals(d.getTime(2), -1463369845020L);
        Assert.assertEquals(d.getTime(3), -1463369845029L);
        Assert.assertEquals(d.getInt(1), 0);

        Assert.assertEquals(d.getTime(4), 1463369845030L);
        Assert.assertEquals(d.getTime(5), -1463369845030L);
        Assert.assertEquals(d.getInt(2), 500);

        Assert.assertEquals(d.getTime(6), -1463369845031L);
        Assert.assertEquals(d.getTime(7), -1463369845039L);
        Assert.assertEquals(d.getInt(3), 0);

        Assert.assertEquals(d.getTime(8), 1463369845040L);
        Assert.assertEquals(d.getTime(9), -1463369845040L);
        Assert.assertEquals(d.getInt(4), 500);

        Assert.assertEquals(d.getTime(10), -1463369845041L);
        Assert.assertEquals(d.getTime(11), -1463369845044L);
        Assert.assertEquals(d.getInt(5), 400);

        Assert.assertEquals(d.getTime(12), 1463369845045L);
        Assert.assertEquals(d.getTime(13), 1463369845050L);
        Assert.assertEquals(d.getInt(6), 400);

        Assert.assertEquals(d.getTime(14), 1463369845051L);
        Assert.assertEquals(d.getTime(15), 1463369845055L);
        Assert.assertEquals(d.getInt(7), 100);

        Assert.assertEquals(d.getTime(16), 1463369845060L);
        Assert.assertEquals(d.getTime(17), 1463369845065L);
        Assert.assertEquals(d.getInt(8), 500);

        Assert.assertEquals(d.getTime(18), 1463369845066L);
        Assert.assertEquals(d.getTime(19), 1463369845070L);
        Assert.assertEquals(d.getInt(9), 300);

        Assert.assertEquals(d.getTime(20), 1463369845071L);
        Assert.assertEquals(d.getTime(21), 1463369845074L);
        Assert.assertEquals(d.getInt(10), 100);

        Assert.assertEquals(d.getTime(22), 1463369845075L);
        Assert.assertEquals(d.getTime(23), 1463369845085L);
        Assert.assertEquals(d.getInt(11), 500);

        Assert.assertEquals(d.getTime(24), 1463369845086L);
        Assert.assertEquals(d.getTime(25), 1463369845090L);
        Assert.assertEquals(d.getInt(12), 100);

        Assert.assertEquals(d.getTime(26), 1463369845095L);
        Assert.assertEquals(d.getTime(27), -1463369845095L);
        Assert.assertEquals(d.getInt(13), 500);

        Assert.assertEquals(d.getTime(28), 1463369845096L);
        Assert.assertEquals(d.getTime(29), 1463369845104L);
        Assert.assertEquals(d.getInt(14), 300);

        Assert.assertEquals(d.getTime(30), 1463369845105L);
        Assert.assertEquals(d.getTime(31), -1463369845105L);
        Assert.assertEquals(d.getInt(14), 300);

        Assert.assertEquals(d.getTime(32), 1463369845106L);
        Assert.assertEquals(d.getTime(33), 1463369845109L);
        Assert.assertEquals(d.getInt(16), 300);

        Assert.assertEquals(d.getTime(34), 1463369845110L);
        Assert.assertEquals(d.getTime(35), -1463369845110L);
        Assert.assertEquals(d.getInt(17), 300);
    }

    @Test
    public void longMixTest() throws IOException {
        IntervalTreeOperation tree = new IntervalTreeOperation(TSDataType.INT64);

        // old : [1, 10]
        // new : [5, 10]
        tree.update(5L, 10L, l2);
        DynamicOneColumnData memoryData = tree.queryMemory(null);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        writeTimePair(outputStream, 1L, 10L, l1);
        outputStream.flush();
        byte[] page = outputStream.toByteArray();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(page);
        DynamicOneColumnData mergeBlockAns = tree.queryFileBlock(null, null, null, inputStream, memoryData);
        Assert.assertEquals(mergeBlockAns.valueLength, 2);
        for (int i = 0; i < mergeBlockAns.valueLength; i++) {
            if (i == 0) {
                Assert.assertEquals(mergeBlockAns.getTime(i * 2), 1);
                Assert.assertEquals(mergeBlockAns.getTime(i * 2 + 1), 4);
                Assert.assertEquals(mergeBlockAns.getLong(i), 1L);
            } else if (i == 1) {
                Assert.assertEquals(mergeBlockAns.getTime(i * 2), 5);
                Assert.assertEquals(mergeBlockAns.getTime(i * 2 + 1), 10);
                Assert.assertEquals(mergeBlockAns.getLong(i), 2L);
            }
        }
    }

    @Test
    public void floatMixTest() throws IOException {
        IntervalTreeOperation tree = new IntervalTreeOperation(TSDataType.FLOAT);

        // old : [1, 10]
        // new : [5, 10]
        tree.update(5L, 10L, f2);
        DynamicOneColumnData memoryData = tree.queryMemory(null);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        writeTimePair(outputStream, 1L, 10L, f1);
        outputStream.flush();
        byte[] page = outputStream.toByteArray();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(page);
        DynamicOneColumnData mergeBlockAns = tree.queryFileBlock(null, null, null, inputStream, memoryData);
        Assert.assertEquals(mergeBlockAns.valueLength, 2);
        for (int i = 0; i < mergeBlockAns.valueLength; i++) {
            if (i == 0) {
                Assert.assertEquals(mergeBlockAns.getTime(i * 2), 1);
                Assert.assertEquals(mergeBlockAns.getTime(i * 2 + 1), 4);
                Assert.assertEquals(mergeBlockAns.getFloat(i), 1.0f, delta);
            } else if (i == 1) {
                Assert.assertEquals(mergeBlockAns.getTime(i * 2), 5);
                Assert.assertEquals(mergeBlockAns.getTime(i * 2 + 1), 10);
                Assert.assertEquals(mergeBlockAns.getFloat(i), 2.0f, delta);
            }
        }
    }

    @Test
    public void doubleMixTest() throws IOException {
        IntervalTreeOperation tree = new IntervalTreeOperation(TSDataType.DOUBLE);

        // old : [1, 10]
        // new : [5, 10]
        tree.update(5L, 10L, d2);
        DynamicOneColumnData memoryData = tree.queryMemory(null);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        writeTimePair(outputStream, 1L, 10L, d1);
        outputStream.flush();
        byte[] page = outputStream.toByteArray();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(page);
        DynamicOneColumnData mergeBlockAns = tree.queryFileBlock(null, null, null, inputStream, memoryData);
        Assert.assertEquals(mergeBlockAns.valueLength, 2);
        for (int i = 0; i < mergeBlockAns.valueLength; i++) {
            if (i == 0) {
                Assert.assertEquals(mergeBlockAns.getTime(i * 2), 1);
                Assert.assertEquals(mergeBlockAns.getTime(i * 2 + 1), 4);
                Assert.assertEquals(mergeBlockAns.getDouble(i), 1.0, delta);
            } else if (i == 1) {
                Assert.assertEquals(mergeBlockAns.getTime(i * 2), 5);
                Assert.assertEquals(mergeBlockAns.getTime(i * 2 + 1), 10);
                Assert.assertEquals(mergeBlockAns.getDouble(i), 2.0, delta);
            }
        }
    }

    @Test
    public void stringMixTest() throws IOException {
        IntervalTreeOperation newTree = new IntervalTreeOperation(TSDataType.TEXT);

        // old : [1, 10] "s1"
        // new : [5, 10] "s2"
        newTree.update(5L, 10L, s2);
        DynamicOneColumnData memoryData = newTree.queryMemory(null);

        IntervalTreeOperation oldTree = new IntervalTreeOperation(TSDataType.TEXT);
        oldTree.update(1L, 10L, s1);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        oldTree.toBytes(outputStream);
        byte[] page = outputStream.toByteArray();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(page);
        DynamicOneColumnData mergeBlockAns = newTree.queryFileBlock(null, null, null, inputStream, memoryData);
        Assert.assertEquals(mergeBlockAns.valueLength, 2);
        for (int i = 0; i < mergeBlockAns.valueLength; i++) {
            if (i == 0) {
                Assert.assertEquals(mergeBlockAns.getTime(i * 2), 1);
                Assert.assertEquals(mergeBlockAns.getTime(i * 2 + 1), 4);
                Assert.assertEquals(mergeBlockAns.getBinary(i).getStringValue(), "s1");
            } else if (i == 1) {
                Assert.assertEquals(mergeBlockAns.getTime(i * 2), 5);
                Assert.assertEquals(mergeBlockAns.getTime(i * 2 + 1), 10);
                Assert.assertEquals(mergeBlockAns.getBinary(i).getStringValue(), "s2");
            }
        }
    }

    private void writeTimePair(ByteArrayOutputStream out, long s, long e) throws IOException {
        out.write(BytesUtils.longToBytes(s));
        out.write(BytesUtils.longToBytes(e));
    }

    private void writeTimePair(ByteArrayOutputStream out, long s, long e, byte[] value) throws IOException {
        out.write(BytesUtils.longToBytes(s));
        out.write(BytesUtils.longToBytes(e));
        out.write(value);
    }

    @Test
    public void intervalRelationBugFixTest() throws IOException {

        // old : [5, 10]
        // new : [1, 10]
        IntervalTreeOperation tree = new IntervalTreeOperation(TSDataType.INT32);
        tree.update(5L, 10L, i2);
        DynamicOneColumnData memoryData = tree.queryMemory(null);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        writeTimePair(outputStream, 1L, 10L, i1);
        outputStream.flush();
        byte[] page = outputStream.toByteArray();
        ByteArrayInputStream inputStream = new ByteArrayInputStream(page);
        DynamicOneColumnData mergeBlockAns = tree.queryFileBlock(null, null, null, inputStream, memoryData);
        Assert.assertEquals(mergeBlockAns.valueLength, 2);
        for (int i = 0; i < mergeBlockAns.valueLength; i++) {
            if (i == 0) {
                Assert.assertEquals(mergeBlockAns.getTime(i * 2), 1);
                Assert.assertEquals(mergeBlockAns.getTime(i * 2 + 1), 4);
                Assert.assertEquals(mergeBlockAns.getInt(i), 1);
            } else if (i == 1) {
                Assert.assertEquals(mergeBlockAns.getTime(i * 2), 5);
                Assert.assertEquals(mergeBlockAns.getTime(i * 2 + 1), 10);
                Assert.assertEquals(mergeBlockAns.getInt(i), 2);
            }
        }

        // old : [5, 5] INSERT
        // new : [5, 5] UPDATE
        tree.reset();
        tree.update(5L, 5L, i2);
        memoryData = tree.queryMemory(null);
        outputStream = new ByteArrayOutputStream();
        writeTimePair(outputStream, 5L, -5L, i1);
        outputStream.flush();
        page = outputStream.toByteArray();
        inputStream = new ByteArrayInputStream(page);
        mergeBlockAns = tree.queryFileBlock(null, null, null, inputStream, memoryData);
        for (int i = 0; i < mergeBlockAns.valueLength; i++) {
            if (i == 0) {
                Assert.assertEquals(mergeBlockAns.getTime(i * 2), 5);
                Assert.assertEquals(mergeBlockAns.getTime(i * 2 + 1), -5);
                Assert.assertEquals(mergeBlockAns.getInt(i), 2);
            }
        }

        // old : [0, 10] DELETE
        // new : [5, 15] UPDATE
        // RFIRSTCORSS
        tree.reset();
        tree.update(5L, 15L, i2);
        memoryData = tree.queryMemory(null);
        outputStream = new ByteArrayOutputStream();
        writeTimePair(outputStream, 0L, -10L); // DELETE Operation don't store value bytes.
        outputStream.flush();
        page = outputStream.toByteArray();
        inputStream = new ByteArrayInputStream(page);
        mergeBlockAns = tree.queryFileBlock(null, null, null, inputStream, memoryData);
        Assert.assertEquals(mergeBlockAns.valueLength, 2);
        for (int i = 0; i < mergeBlockAns.valueLength; i++) {
            // outputDynamicOneColumn(mergeBlockAns, i);
            if (i == 0) {
                Assert.assertEquals(mergeBlockAns.getTime(i * 2), 0);
                Assert.assertEquals(mergeBlockAns.getTime(i * 2 + 1), -4);
                Assert.assertEquals(mergeBlockAns.getInt(i), 0);
            } else if (i == 1) {
                Assert.assertEquals(mergeBlockAns.getTime(i * 2), 11);
                Assert.assertEquals(mergeBlockAns.getTime(i * 2 + 1), 15);
                Assert.assertEquals(mergeBlockAns.getInt(i), 2);
            }
        }

        // old : [0, 10] DELETE
        // new : [0, 10] UPDATE
        // RFIRSTCORSS
        tree.reset();
        tree.update(0L, 10L, i2);
        memoryData = tree.queryMemory(null);
        outputStream = new ByteArrayOutputStream();
        writeTimePair(outputStream, 0L, -10L); // DELETE Operation don't store value bytes.
        outputStream.flush();
        page = outputStream.toByteArray();
        inputStream = new ByteArrayInputStream(page);
        mergeBlockAns = tree.queryFileBlock(null, null, null, inputStream, memoryData);
        for (int i = 0; i < mergeBlockAns.valueLength; i++) {
            Assert.assertEquals(mergeBlockAns.getTime(i * 2), 0);
            Assert.assertEquals(mergeBlockAns.getTime(i * 2 + 1), -10);
            Assert.assertEquals(mergeBlockAns.getInt(i), 0);
        }

        // old : [5, 10] DELETE
        // new : [1, 10] UPDATE
        // LCOVERSR
        tree.reset();
        tree.update(1L, 10L, i2);
        memoryData = tree.queryMemory(null);
        outputStream = new ByteArrayOutputStream();
        writeTimePair(outputStream, -5L, -10L); // DELETE Operation don't store value bytes.
        outputStream.flush();
        page = outputStream.toByteArray();
        inputStream = new ByteArrayInputStream(page);
        mergeBlockAns = tree.queryFileBlock(null, null, null, inputStream, memoryData);
        for (int i = 0; i < mergeBlockAns.valueLength; i++) {
            Assert.assertEquals(mergeBlockAns.getTime(i * 2), 1);
            Assert.assertEquals(mergeBlockAns.getTime(i * 2 + 1), 4);
            Assert.assertEquals(mergeBlockAns.getInt(i), 2);
        }

        // old : [5, 10] DELETE
        // new : [1, 15] UPDATE
        // LCOVERSR
        tree.reset();
        tree.update(1L, 15L, i2);
        memoryData = tree.queryMemory(null);
        outputStream = new ByteArrayOutputStream();
        writeTimePair(outputStream, -5L, -10L); // DELETE Operation don't store value bytes.
        outputStream.flush();
        page = outputStream.toByteArray();
        inputStream = new ByteArrayInputStream(page);
        mergeBlockAns = tree.queryFileBlock(null, null, null, inputStream, memoryData);
        for (int i = 0; i < mergeBlockAns.valueLength; i++) {
            // outputDynamicOneColumn(mergeBlockAns, i);
            if (i == 0) {
                Assert.assertEquals(mergeBlockAns.getTime(i * 2), 1);
                Assert.assertEquals(mergeBlockAns.getTime(i * 2 + 1), 4);
                Assert.assertEquals(mergeBlockAns.getInt(i), 2);
            } else if (i == 2) {
                Assert.assertEquals(mergeBlockAns.getTime(i * 2), 11);
                Assert.assertEquals(mergeBlockAns.getTime(i * 2 + 1), 15);
                Assert.assertEquals(mergeBlockAns.getInt(i), 2);
            }
        }

        // old : [0, 20] DELETE
        // new : [0, 10] UPDATE
        // old COVERS new
        tree.reset();
        tree.update(0L, 10L, i2);
        memoryData = tree.queryMemory(null);
        outputStream = new ByteArrayOutputStream();
        writeTimePair(outputStream, 0L, -20L); // DELETE Operation don't store value bytes.
        outputStream.flush();
        page = outputStream.toByteArray();
        inputStream = new ByteArrayInputStream(page);
        mergeBlockAns = tree.queryFileBlock(null, null, null, inputStream, memoryData);
        Assert.assertEquals(mergeBlockAns.valueLength, 2);
        for (int i = 0; i < mergeBlockAns.valueLength; i++) {
            // outputDynamicOneColumn(mergeBlockAns, i);
            if (i == 0) {
                Assert.assertEquals(mergeBlockAns.getTime(i * 2), 0);
                Assert.assertEquals(mergeBlockAns.getTime(i * 2 + 1), -10);
                Assert.assertEquals(mergeBlockAns.getInt(i), 0);
            } else if (i == 2) {
                Assert.assertEquals(mergeBlockAns.getTime(i * 2), -11);
                Assert.assertEquals(mergeBlockAns.getTime(i * 2 + 1), -20);
                Assert.assertEquals(mergeBlockAns.getInt(i), 0);
            }
        }

        // old : [0, 20] DELETE
        // new : [5, 20] UPDATE
        // old COVERS new
        tree.reset();
        tree.update(5L, 20L, i2);
        memoryData = tree.queryMemory(null);
        outputStream = new ByteArrayOutputStream();
        writeTimePair(outputStream, 0L, -20L); // DELETE Operation don't store value bytes.
        outputStream.flush();
        page = outputStream.toByteArray();
        inputStream = new ByteArrayInputStream(page);
        mergeBlockAns = tree.queryFileBlock(null, null, null, inputStream, memoryData);
        Assert.assertEquals(mergeBlockAns.valueLength, 1);
        for (int i = 0; i < mergeBlockAns.valueLength; i++) {
             // outputDynamicOneColumn(mergeBlockAns, i);
            if (i == 0) {
                Assert.assertEquals(mergeBlockAns.getTime(i * 2), 0);
                Assert.assertEquals(mergeBlockAns.getTime(i * 2 + 1), -20);
                Assert.assertEquals(mergeBlockAns.getInt(i), 0);
            }
        }

        // old : [1, 5] UPDATE
        // new : [5, 5] INSERT
        // old COVERS new
        tree.reset();
        tree.insert(5L, i2);
        memoryData = tree.queryMemory(null);
        outputStream = new ByteArrayOutputStream();
        writeTimePair(outputStream, 1L, 5L, i1); // DELETE Operation don't store value bytes.
        outputStream.flush();
        page = outputStream.toByteArray();
        inputStream = new ByteArrayInputStream(page);
        mergeBlockAns = tree.queryFileBlock(null, null, null, inputStream, memoryData);
        Assert.assertEquals(mergeBlockAns.valueLength, 2);
        for (int i = 0; i < mergeBlockAns.valueLength; i++) {
            // outputDynamicOneColumn(mergeBlockAns, i);
            if (i == 0) {
                Assert.assertEquals(mergeBlockAns.getTime(i * 2), 1);
                Assert.assertEquals(mergeBlockAns.getTime(i * 2 + 1), 4);
                Assert.assertEquals(mergeBlockAns.getInt(i), 1);
            } else if (i == 1) {
                Assert.assertEquals(mergeBlockAns.getTime(i * 2), 5);
                Assert.assertEquals(mergeBlockAns.getTime(i * 2 + 1), -5);
                Assert.assertEquals(mergeBlockAns.getInt(i), 2);
            }
        }

        // old : [1, 1] INSERT
        // new : [1, 5] UPDATE
        // new COVERS old
        tree.reset();
        tree.update(1L, 5L, i2);
        memoryData = tree.queryMemory(null);
        outputStream = new ByteArrayOutputStream();
        writeTimePair(outputStream, 1L, -1L, i1); // DELETE Operation don't store value bytes.
        outputStream.flush();
        page = outputStream.toByteArray();
        inputStream = new ByteArrayInputStream(page);
        mergeBlockAns = tree.queryFileBlock(null, null, null, inputStream, memoryData);
        Assert.assertEquals(mergeBlockAns.valueLength, 2);
        for (int i = 0; i < mergeBlockAns.valueLength; i++) {
            // outputDynamicOneColumn(mergeBlockAns, i);
            if (i == 0) {
                Assert.assertEquals(mergeBlockAns.getTime(i * 2), 1);
                Assert.assertEquals(mergeBlockAns.getTime(i * 2 + 1), -1);
                Assert.assertEquals(mergeBlockAns.getInt(i), 2);
            } else if (i == 1) {
                Assert.assertEquals(mergeBlockAns.getTime(i * 2), 2);
                Assert.assertEquals(mergeBlockAns.getTime(i * 2 + 1), 5);
                Assert.assertEquals(mergeBlockAns.getInt(i), 2);
            }
        }

        // old : [1, 5] UPDATE
        // new : [1, 5] UPDATE
        // new equals old
        tree.reset();
        tree.update(1L, 5L, i2);
        memoryData = tree.queryMemory(null);
        outputStream = new ByteArrayOutputStream();
        writeTimePair(outputStream, 1L, 5L, i1); // DELETE Operation don't store value bytes.
        outputStream.flush();
        page = outputStream.toByteArray();
        inputStream = new ByteArrayInputStream(page);
        mergeBlockAns = tree.queryFileBlock(null, null, null, inputStream, memoryData);
        Assert.assertEquals(mergeBlockAns.valueLength, 1);
        for (int i = 0; i < mergeBlockAns.valueLength; i++) {
            if (i == 0) {
                Assert.assertEquals(mergeBlockAns.getTime(i * 2), 1);
                Assert.assertEquals(mergeBlockAns.getTime(i * 2 + 1), 5);
                Assert.assertEquals(mergeBlockAns.getInt(i), 2);
            }
        }

        // old : [1, 5] UPDATE
        // new : [2, 4] UPDATE
        // old covers new
        tree.reset();
        tree.update(2L, 4L, i2);
        memoryData = tree.queryMemory(null);
        outputStream = new ByteArrayOutputStream();
        writeTimePair(outputStream, 1L, 5L, i1); // DELETE Operation don't store value bytes.
        outputStream.flush();
        page = outputStream.toByteArray();
        inputStream = new ByteArrayInputStream(page);
        mergeBlockAns = tree.queryFileBlock(null, null, null, inputStream, memoryData);
        Assert.assertEquals(mergeBlockAns.valueLength, 3);
        for (int i = 0; i < mergeBlockAns.valueLength; i++) {
            // outputDynamicOneColumn(mergeBlockAns, i);
            if (i == 0) {
                Assert.assertEquals(mergeBlockAns.getTime(i * 2), 1);
                Assert.assertEquals(mergeBlockAns.getTime(i * 2 + 1), 1);
                Assert.assertEquals(mergeBlockAns.getInt(i), 1);
            } else if (i == 1) {
                Assert.assertEquals(mergeBlockAns.getTime(i * 2), 2);
                Assert.assertEquals(mergeBlockAns.getTime(i * 2 + 1), 4);
                Assert.assertEquals(mergeBlockAns.getInt(i), 2);
            } else if (i == 2) {
                Assert.assertEquals(mergeBlockAns.getTime(i * 2), 5);
                Assert.assertEquals(mergeBlockAns.getTime(i * 2 + 1), 5);
                Assert.assertEquals(mergeBlockAns.getInt(i), 1);
            }
        }

        // old : [1, 5] [6, 7] UPDATE
        // new : [2, 4] UPDATE
        // old covers new
        tree.reset();
        tree.update(2L, 4L, i2);
        memoryData = tree.queryMemory(null);
        outputStream = new ByteArrayOutputStream();
        writeTimePair(outputStream, 1L, 5L, i1); // DELETE Operation don't store value bytes.
        writeTimePair(outputStream, 6L, 7L, i3);
        outputStream.flush();
        page = outputStream.toByteArray();
        inputStream = new ByteArrayInputStream(page);
        mergeBlockAns = tree.queryFileBlock(null, null, null, inputStream, memoryData);
        Assert.assertEquals(mergeBlockAns.valueLength, 4);
        for (int i = 0; i < mergeBlockAns.valueLength; i++) {
            // outputDynamicOneColumn(mergeBlockAns, i);
            if (i == 0) {
                Assert.assertEquals(mergeBlockAns.getTime(i * 2), 1);
                Assert.assertEquals(mergeBlockAns.getTime(i * 2 + 1), 1);
                Assert.assertEquals(mergeBlockAns.getInt(i), 1);
            } else if (i == 1) {
                Assert.assertEquals(mergeBlockAns.getTime(i * 2), 2);
                Assert.assertEquals(mergeBlockAns.getTime(i * 2 + 1), 4);
                Assert.assertEquals(mergeBlockAns.getInt(i), 2);
            } else if (i == 2) {
                Assert.assertEquals(mergeBlockAns.getTime(i * 2), 5);
                Assert.assertEquals(mergeBlockAns.getTime(i * 2 + 1), 5);
                Assert.assertEquals(mergeBlockAns.getInt(i), 1);
            } else if (i == 3) {
                Assert.assertEquals(mergeBlockAns.getTime(i * 2), 6);
                Assert.assertEquals(mergeBlockAns.getTime(i * 2 + 1), 7);
                Assert.assertEquals(mergeBlockAns.getInt(i), 3);
            }
        }
    }

    // System.out.println(mergeBlockAns.getTime(i * 2) + "," + mergeBlockAns.getTime(i * 2) + "," + mergeBlockAns.getInt(i));

    @Test
    public void getDynamicListTest() throws IOException {

        IntervalTreeOperation tree = new IntervalTreeOperation(TSDataType.INT32);
        byte[] b1 = BytesUtils.intToBytes(100);
        byte[] b2 = BytesUtils.intToBytes(200);
        byte[] b3 = BytesUtils.intToBytes(300);
        byte[] b4 = BytesUtils.intToBytes(400);
        byte[] b5 = BytesUtils.intToBytes(500);

        ByteArrayInputStream[] ins = new ByteArrayInputStream[10];
        //  (1)
        tree.insert(1463369845000L, b1);
        tree.update(1463369845005L, 1463369845010L, b1);
        tree.update(1463369845015L, 1463369845025L, b1);
        tree.update(1463369845030L, 1463369845055L, b1);
        tree.update(1463369845070L, 1463369845080L, b1);
        tree.update(1463369845083L, 1463369845090L, b1);
        tree.insert(1463369845095L, b1);
        tree.insert(1463369845105L, b1);
        tree.insert(1463369845110L, b1);

        ByteArrayOutputStream out1 = new ByteArrayOutputStream();
        tree.toBytes(out1);

        byte[] page = out1.toByteArray();
        ins[1] = new ByteArrayInputStream(page);

        // (2)
        tree.update(1463369845100L, 1463369845110L, b2);
        ByteArrayOutputStream out2 = new ByteArrayOutputStream();
        tree.toBytes(out2);
        page = out2.toByteArray();
        ins[2] = new ByteArrayInputStream(page);

        // (3)
        tree.delete(1463369845044L);
        tree.update(1463369845060L, 1463369845070L, b3);
        tree.update(1463369845095L, 1463369845110L, b3);
        ByteArrayOutputStream out3 = new ByteArrayOutputStream();
        tree.toBytes(out3);
        page = out3.toByteArray();
        ins[3] = new ByteArrayInputStream(page);

        // (4)
        tree.update(1463369845035L, 1463369845050L, b4);
        ByteArrayOutputStream out4 = new ByteArrayOutputStream();
        tree.toBytes(out4);
        page = out4.toByteArray();
        ins[4] = new ByteArrayInputStream(page);

        // (5)
        tree.delete(1463369845019L);
        tree.insert(1463369845030L, b5);
        tree.insert(1463369845040L, b5);
        tree.update(1463369845060L, 1463369845065L, b5);
        tree.update(1463369845075L, 1463369845085L, b5);
        tree.insert(1463369845095L, b5);

        //  calc
        DynamicOneColumnData doc = tree.queryMemory(null);

        for (int i = 4; i >= 1; i--) {
            doc = tree.queryFileBlock(null, null, null, ins[i], doc);
        }

        GtEq<Long> timeFilter =  FilterFactory.gtEq(FilterFactory.longFilterSeries(
                "d1", "s1", FilterSeriesType.TIME_FILTER), 0L, true);
        GtEq<Integer> valueFilter =  FilterFactory.gtEq(FilterFactory.intFilterSeries(
                "d1", "s1", FilterSeriesType.VALUE_FILTER), 0, true);

        List<Object> ans = tree.getDynamicList(timeFilter, valueFilter, null, doc);
        DynamicOneColumnData insertAdopt = (DynamicOneColumnData) ans.get(0);
        DynamicOneColumnData updateAdopt = (DynamicOneColumnData) ans.get(1);

        for(int i = 0;i < insertAdopt.valueLength;i++) {
            //LOG.info(insertAdopt.getTime(i) + "," + insertAdopt.getInt(i));
            if (i == 0) {
                Assert.assertEquals(insertAdopt.getTime(i), 1463369845030L);
                Assert.assertEquals(insertAdopt.getInt(i), 500);
            } else if (i == 1) {
                Assert.assertEquals(insertAdopt.getTime(i), 1463369845040L);
                Assert.assertEquals(insertAdopt.getInt(i), 500);
            } else if (i == 2) {
                Assert.assertEquals(insertAdopt.getTime(i), 1463369845095L);
                Assert.assertEquals(insertAdopt.getInt(i), 500);
            } else if (i == 3) {
                Assert.assertEquals(insertAdopt.getTime(i), 1463369845105L);
                Assert.assertEquals(insertAdopt.getInt(i), 300);
            } else if (i == 4) {
                Assert.assertEquals(insertAdopt.getTime(i), 1463369845110L);
                Assert.assertEquals(insertAdopt.getInt(i), 300);
            }
        }

        Assert.assertEquals(updateAdopt.valueLength, 9);
        for(int i = 0;i < updateAdopt.valueLength;i++) {
            if (i == 0) {
                Assert.assertEquals(updateAdopt.getTime(i*2), 1463369845045L);
                Assert.assertEquals(updateAdopt.getTime(i*2+1), 1463369845050L);
                Assert.assertEquals(updateAdopt.getInt(i), 400);
            } else if (i == 1) {
                Assert.assertEquals(updateAdopt.getTime(i*2), 1463369845051L);
                Assert.assertEquals(updateAdopt.getTime(i*2+1), 1463369845055L);
                Assert.assertEquals(updateAdopt.getInt(i), 100);
            } else if (i == 2) {
                Assert.assertEquals(updateAdopt.getTime(i*2), 1463369845060L);
                Assert.assertEquals(updateAdopt.getTime(i*2+1), 1463369845065L);
                Assert.assertEquals(updateAdopt.getInt(i), 500);
            } else if (i == 3) {
                Assert.assertEquals(updateAdopt.getTime(i*2), 1463369845066L);
                Assert.assertEquals(updateAdopt.getTime(i*2+1), 1463369845070L);
                Assert.assertEquals(updateAdopt.getInt(i), 300);
            } else if (i == 4) {
                Assert.assertEquals(updateAdopt.getTime(i*2), 1463369845071L);
                Assert.assertEquals(updateAdopt.getTime(i*2+1), 1463369845074L);
                Assert.assertEquals(updateAdopt.getInt(i), 100);
            } else if (i == 5) {
                Assert.assertEquals(updateAdopt.getTime(i*2), 1463369845075L);
                Assert.assertEquals(updateAdopt.getTime(i*2+1), 1463369845085L);
                Assert.assertEquals(updateAdopt.getInt(i), 500);
            } else if (i == 6) {
                Assert.assertEquals(updateAdopt.getTime(i*2), 1463369845086L);
                Assert.assertEquals(updateAdopt.getTime(i*2+1), 1463369845090L);
                Assert.assertEquals(updateAdopt.getInt(i), 100);
            } else if (i == 7) {
                Assert.assertEquals(updateAdopt.getTime(i*2), 1463369845096L);
                Assert.assertEquals(updateAdopt.getTime(i*2+1), 1463369845104L);
                Assert.assertEquals(updateAdopt.getInt(i), 300);
            } else if (i == 8) {
                Assert.assertEquals(updateAdopt.getTime(i*2), 1463369845106L);
                Assert.assertEquals(updateAdopt.getTime(i*2+1), 1463369845109L);
                Assert.assertEquals(updateAdopt.getInt(i), 300);
            }
        }
    }
}