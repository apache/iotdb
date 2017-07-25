package cn.edu.thu.tsfiledb.engine.overflow;

import cn.edu.thu.tsfile.common.utils.BytesUtils;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.thu.tsfiledb.engine.overflow.IntervalTreeOperation;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * @author CGF.
 */
public class IntervalTreeOperationMemoryTest {
    private static byte[] i1 = BytesUtils.intToBytes(1);
    private static byte[] i2 = BytesUtils.intToBytes(2);
    private static byte[] i3 = BytesUtils.intToBytes(3);

    private static byte[] l1 = BytesUtils.longToBytes(1L);
    private static byte[] l2 = BytesUtils.longToBytes(2L);

    private static byte[] f1 = BytesUtils.floatToBytes(1.0f);
    private static byte[] f2 = BytesUtils.floatToBytes(2.0f);

    private static byte[] d1 = BytesUtils.doubleToBytes(1.0);
    private static byte[] d2 = BytesUtils.doubleToBytes(2.0);

    private static double delta = 0.000001;

    @Test
    public void intMixTest() throws IOException {
        IntervalTreeOperation[] tree = new IntervalTreeOperation[6];
        byte[] b1 = BytesUtils.intToBytes(100);
        byte[] b2 = BytesUtils.intToBytes(200);
        byte[] b3 = BytesUtils.intToBytes(300);
        byte[] b4 = BytesUtils.intToBytes(400);
        byte[] b5 = BytesUtils.intToBytes(500);

        //  (1)
        tree[1] = new IntervalTreeOperation(TSDataType.INT32);
        tree[1].insert(1463369845000L, b1);
        tree[1].update(1463369845005L, 1463369845010L, b1);
        tree[1].update(1463369845015L, 1463369845025L, b1);
        tree[1].update(1463369845030L, 1463369845055L, b1);
        tree[1].update(1463369845070L, 1463369845080L, b1);
        tree[1].update(1463369845083L, 1463369845090L, b1);
        tree[1].insert(1463369845095L, b1);
        tree[1].insert(1463369845105L, b1);
        tree[1].insert(1463369845110L, b1);

        // (2)
        tree[2] = new IntervalTreeOperation(TSDataType.INT32);
        tree[2].update(1463369845100L, 1463369845110L, b2);

        // (3)
        tree[3] = new IntervalTreeOperation(TSDataType.INT32);
        tree[3].delete(1463369845045L);
        tree[3].update(1463369845060L, 1463369845070L, b3);
        tree[3].update(1463369845095L, 1463369845110L, b3);


        // (4)
        tree[4] = new IntervalTreeOperation(TSDataType.INT32);
        tree[4].update(1463369845035L, 1463369845050L, b4);

        // (5)
        tree[5] = new IntervalTreeOperation(TSDataType.INT32);
        tree[5].delete(1463369845020L);
        tree[5].insert(1463369845030L, b5);
        tree[5].insert(1463369845040L, b5);
        tree[5].update(1463369845060L, 1463369845065L, b5);
        tree[5].update(1463369845075L, 1463369845085L, b5);
        tree[5].insert(1463369845095L, b5);

        //  calc
        DynamicOneColumnData doc = tree[5].queryMemory(null, null, null,  null);

        for (int i = 4; i >= 1; i--) {
            doc = tree[i].queryMemory(null, null, null, doc);
        }

//        for (int i = 0; i < d.valueLength; i++) {
//            System.out.println(d.getTime(i * 2) + " " + d.getTime(i * 2 + 1) + " " + d.getInt(i));
//        }
        Assert.assertEquals(doc.valueLength, 18);
        Assert.assertEquals(doc.getTime(0), 0L);
        Assert.assertEquals(doc.getTime(1), -1463369845019L);
        Assert.assertEquals(doc.getInt(0), 0);

        Assert.assertEquals(doc.getTime(2), -1463369845020L);
        Assert.assertEquals(doc.getTime(3), -1463369845029L);
        Assert.assertEquals(doc.getInt(1), 0);

        Assert.assertEquals(doc.getTime(4), 1463369845030L);
        Assert.assertEquals(doc.getTime(5), -1463369845030L);
        Assert.assertEquals(doc.getInt(2), 500);

        Assert.assertEquals(doc.getTime(6), -1463369845031L);
        Assert.assertEquals(doc.getTime(7), -1463369845039L);
        Assert.assertEquals(doc.getInt(3), 0);

        Assert.assertEquals(doc.getTime(8), 1463369845040L);
        Assert.assertEquals(doc.getTime(9), -1463369845040L);
        Assert.assertEquals(doc.getInt(4), 500);

        Assert.assertEquals(doc.getTime(10), -1463369845041L);
        Assert.assertEquals(doc.getTime(11), -1463369845044L);
        Assert.assertEquals(doc.getInt(5), 400);

        Assert.assertEquals(doc.getTime(12), 1463369845045L);
        Assert.assertEquals(doc.getTime(13), 1463369845050L);
        Assert.assertEquals(doc.getInt(6), 400);

        Assert.assertEquals(doc.getTime(14), 1463369845051L);
        Assert.assertEquals(doc.getTime(15), 1463369845055L);
        Assert.assertEquals(doc.getInt(7), 100);

        Assert.assertEquals(doc.getTime(16), 1463369845060L);
        Assert.assertEquals(doc.getTime(17), 1463369845065L);
        Assert.assertEquals(doc.getInt(8), 500);

        Assert.assertEquals(doc.getTime(18), 1463369845066L);
        Assert.assertEquals(doc.getTime(19), 1463369845070L);
        Assert.assertEquals(doc.getInt(9), 300);

        Assert.assertEquals(doc.getTime(20), 1463369845071L);
        Assert.assertEquals(doc.getTime(21), 1463369845074L);
        Assert.assertEquals(doc.getInt(10), 100);

        Assert.assertEquals(doc.getTime(22), 1463369845075L);
        Assert.assertEquals(doc.getTime(23), 1463369845085L);
        Assert.assertEquals(doc.getInt(11), 500);

        Assert.assertEquals(doc.getTime(24), 1463369845086L);
        Assert.assertEquals(doc.getTime(25), 1463369845090L);
        Assert.assertEquals(doc.getInt(12), 100);

        Assert.assertEquals(doc.getTime(26), 1463369845095L);
        Assert.assertEquals(doc.getTime(27), -1463369845095L);
        Assert.assertEquals(doc.getInt(13), 500);

        Assert.assertEquals(doc.getTime(28), 1463369845096L);
        Assert.assertEquals(doc.getTime(29), 1463369845104L);
        Assert.assertEquals(doc.getInt(14), 300);

        Assert.assertEquals(doc.getTime(30), 1463369845105L);
        Assert.assertEquals(doc.getTime(31), -1463369845105L);
        Assert.assertEquals(doc.getInt(14), 300);

        Assert.assertEquals(doc.getTime(32), 1463369845106L);
        Assert.assertEquals(doc.getTime(33), 1463369845109L);
        Assert.assertEquals(doc.getInt(16), 300);

        Assert.assertEquals(doc.getTime(34), 1463369845110L);
        Assert.assertEquals(doc.getTime(35), -1463369845110L);
        Assert.assertEquals(doc.getInt(17), 300);
    }

    @Test
    public void longMixTest() throws IOException {

        // new : [5, 10] 2
        // old : [1, 10] 1
        IntervalTreeOperation newTree = new IntervalTreeOperation(TSDataType.INT64);
        newTree.update(5L, 10L, l2);
        DynamicOneColumnData newMemoryData = newTree.queryMemory(null, null, null,  null);

        IntervalTreeOperation oldTree = new IntervalTreeOperation(TSDataType.INT64);
        oldTree.update(1L, 10L, l1);
        DynamicOneColumnData mergeAns = oldTree.queryMemory(null, null, null,  newMemoryData);

        Assert.assertEquals(mergeAns.valueLength, 2);
        for (int i = 0; i < mergeAns.valueLength; i++) {
            if (i == 0) {
                Assert.assertEquals(mergeAns.getTime(i * 2), 1);
                Assert.assertEquals(mergeAns.getTime(i * 2 + 1), 4);
                Assert.assertEquals(mergeAns.getLong(i), 1L);
            } else if (i == 1) {
                Assert.assertEquals(mergeAns.getTime(i * 2), 5);
                Assert.assertEquals(mergeAns.getTime(i * 2 + 1), 10);
                Assert.assertEquals(mergeAns.getLong(i), 2L);
            }
        }
    }

    @Test
    public void floatMixTest() throws IOException {

        // new : [5, 10] 2.0f
        // old : [1, 10] 1.0f
        IntervalTreeOperation newTree = new IntervalTreeOperation(TSDataType.FLOAT);
        newTree.update(5L, 10L, f2);
        DynamicOneColumnData newMemoryData = newTree.queryMemory(null, null, null,  null);

        IntervalTreeOperation oldTree = new IntervalTreeOperation(TSDataType.FLOAT);
        oldTree.update(1L, 10L, f1);
        DynamicOneColumnData mergeAns = oldTree.queryMemory(null, null, null,  newMemoryData);

        Assert.assertEquals(mergeAns.valueLength, 2);
        for (int i = 0; i < mergeAns.valueLength; i++) {
            if (i == 0) {
                Assert.assertEquals(mergeAns.getTime(i * 2), 1);
                Assert.assertEquals(mergeAns.getTime(i * 2 + 1), 4);
                Assert.assertEquals(mergeAns.getFloat(i), 1.0f, delta);
            } else if (i == 1) {
                Assert.assertEquals(mergeAns.getTime(i * 2), 5);
                Assert.assertEquals(mergeAns.getTime(i * 2 + 1), 10);
                Assert.assertEquals(mergeAns.getFloat(i), 2.0f, delta);
            }
        }
    }

    @Test
    public void doubleMixTest() throws IOException {
        // new : [5, 10] 2.0
        // old : [1, 10] 1.0
        IntervalTreeOperation newTree = new IntervalTreeOperation(TSDataType.DOUBLE);
        newTree.update(5L, 10L, d2);
        DynamicOneColumnData newMemoryData = newTree.queryMemory(null, null, null,  null);

        IntervalTreeOperation oldTree = new IntervalTreeOperation(TSDataType.DOUBLE);
        oldTree.update(1L, 10L, d1);
        DynamicOneColumnData mergeAns = oldTree.queryMemory(null, null, null,  newMemoryData);

        Assert.assertEquals(mergeAns.valueLength, 2);
        for (int i = 0; i < mergeAns.valueLength; i++) {
            if (i == 0) {
                Assert.assertEquals(mergeAns.getTime(i * 2), 1);
                Assert.assertEquals(mergeAns.getTime(i * 2 + 1), 4);
                Assert.assertEquals(mergeAns.getDouble(i), 1.0, delta);
            } else if (i == 1) {
                Assert.assertEquals(mergeAns.getTime(i * 2), 5);
                Assert.assertEquals(mergeAns.getTime(i * 2 + 1), 10);
                Assert.assertEquals(mergeAns.getDouble(i), 2.0, delta);
            }
        }
    }

    @Test
    public void intervalRelationBugFixTest() throws IOException {

        // old : [5, 10] 2
        // new : [1, 10] 1
        IntervalTreeOperation newTree = new IntervalTreeOperation(TSDataType.INT32);
        newTree.update(5L, 10L, i2);
        DynamicOneColumnData memoryData = newTree.queryMemory(null, null, null, null);

        IntervalTreeOperation oldTree = new IntervalTreeOperation(TSDataType.INT32);
        oldTree.update(1L, 10L, i1);
        DynamicOneColumnData mergeAnswer = oldTree.queryMemory(null, null, null,  memoryData);
        Assert.assertEquals(mergeAnswer.valueLength, 2);
        for (int i = 0; i < mergeAnswer.valueLength; i++) {
            if (i == 0) {
                Assert.assertEquals(mergeAnswer.getTime(i * 2), 1);
                Assert.assertEquals(mergeAnswer.getTime(i * 2 + 1), 4);
                Assert.assertEquals(mergeAnswer.getInt(i), 1);
            } else if (i == 1) {
                Assert.assertEquals(mergeAnswer.getTime(i * 2), 5);
                Assert.assertEquals(mergeAnswer.getTime(i * 2 + 1), 10);
                Assert.assertEquals(mergeAnswer.getInt(i), 2);
            }
        }

        // new : [5, 5] UPDATE
        // old : [5, 5] INSERT
        newTree.reset();
        newTree.update(5L, 5L, i2);
        memoryData = newTree.queryMemory(null, null, null, null);

        oldTree.reset();
        oldTree.insert(5L, i1);
        mergeAnswer = oldTree.queryMemory(null, null, null,  memoryData);
        for (int i = 0; i < mergeAnswer.valueLength; i++) {
            if (i == 0) {
                Assert.assertEquals(mergeAnswer.getTime(i * 2), 5);
                Assert.assertEquals(mergeAnswer.getTime(i * 2 + 1), -5);
                Assert.assertEquals(mergeAnswer.getInt(i), 2);
            }
        }

        // new : [5, 15] UPDATE
        // old : [0, 10] DELETE
        // RFIRSTCORSS
        newTree.reset();
        newTree.update(5L, 15L, i2);
        memoryData = newTree.queryMemory(null, null, null,  null);

        oldTree.reset();
        oldTree.delete(11L);
        mergeAnswer = oldTree.queryMemory(null, null, null,  memoryData);
        Assert.assertEquals(mergeAnswer.valueLength, 2);
        for (int i = 0; i < mergeAnswer.valueLength; i++) {
            // outputDynamicOneColumn(mergeBlockAns, i);
            if (i == 0) {
                Assert.assertEquals(mergeAnswer.getTime(i * 2), 0);
                Assert.assertEquals(mergeAnswer.getTime(i * 2 + 1), -4);
                Assert.assertEquals(mergeAnswer.getInt(i), 0);
            } else if (i == 1) {
                Assert.assertEquals(mergeAnswer.getTime(i * 2), 11);
                Assert.assertEquals(mergeAnswer.getTime(i * 2 + 1), 15);
                Assert.assertEquals(mergeAnswer.getInt(i), 2);
            }
        }

        // new : [0, 10] UPDATE
        // old : [0, 10] DELETE
        // RFIRSTCORSS
        newTree.reset();
        newTree.update(0L, 10L, i2);
        memoryData = newTree.queryMemory(null, null, null,  null);

        oldTree.reset();
        oldTree.delete(11L);
        mergeAnswer = oldTree.queryMemory(null, null, null,  memoryData);
        for (int i = 0; i < mergeAnswer.valueLength; i++) {
            Assert.assertEquals(mergeAnswer.getTime(i * 2), 0);
            Assert.assertEquals(mergeAnswer.getTime(i * 2 + 1), -10);
            Assert.assertEquals(mergeAnswer.getInt(i), 0);
        }

        // new : [0, 10] UPDATE
        // old : [0, 20] DELETE
        // old COVERS new
        newTree.reset();
        newTree.update(0L, 10L, i2);
        memoryData = newTree.queryMemory(null, null, null, null);

        oldTree.reset();
        oldTree.delete(21L);
        mergeAnswer = oldTree.queryMemory(null, null, null,  memoryData);
        Assert.assertEquals(mergeAnswer.valueLength, 2);
        for (int i = 0; i < mergeAnswer.valueLength; i++) {
            if (i == 0) {
                Assert.assertEquals(mergeAnswer.getTime(i * 2), 0);
                Assert.assertEquals(mergeAnswer.getTime(i * 2 + 1), -10);
                Assert.assertEquals(mergeAnswer.getInt(i), 0);
            } else if (i == 2) {
                Assert.assertEquals(mergeAnswer.getTime(i * 2), -11);
                Assert.assertEquals(mergeAnswer.getTime(i * 2 + 1), -20);
                Assert.assertEquals(mergeAnswer.getInt(i), 0);
            }
        }

        // new : [5, 20] UPDATE
        // old : [0, 20] DELETE
        // old COVERS new
        newTree.reset();
        newTree.update(5L, 20L, i2);
        memoryData = newTree.queryMemory(null, null, null,  null);

        oldTree.reset();
        oldTree.delete(21L);
        mergeAnswer = oldTree.queryMemory(null, null, null,   memoryData);
        Assert.assertEquals(mergeAnswer.valueLength, 1);
        for (int i = 0; i < mergeAnswer.valueLength; i++) {
             // outputDynamicOneColumn(mergeBlockAns, i);
            if (i == 0) {
                Assert.assertEquals(mergeAnswer.getTime(i * 2), 0);
                Assert.assertEquals(mergeAnswer.getTime(i * 2 + 1), -20);
                Assert.assertEquals(mergeAnswer.getInt(i), 0);
            }
        }

        // new : [5, 5] INSERT
        // old : [1, 5] UPDATE
        // old COVERS new
        newTree.reset();
        newTree.insert(5L, i2);
        memoryData = newTree.queryMemory(null, null, null,  null);

        oldTree.reset();
        oldTree.update(1L, 5L, i1);
        mergeAnswer = oldTree.queryMemory(null, null, null,   memoryData);
        Assert.assertEquals(mergeAnswer.valueLength, 2);
        for (int i = 0; i < mergeAnswer.valueLength; i++) {
            // outputDynamicOneColumn(mergeBlockAns, i);
            if (i == 0) {
                Assert.assertEquals(mergeAnswer.getTime(i * 2), 1);
                Assert.assertEquals(mergeAnswer.getTime(i * 2 + 1), 4);
                Assert.assertEquals(mergeAnswer.getInt(i), 1);
            } else if (i == 1) {
                Assert.assertEquals(mergeAnswer.getTime(i * 2), 5);
                Assert.assertEquals(mergeAnswer.getTime(i * 2 + 1), -5);
                Assert.assertEquals(mergeAnswer.getInt(i), 2);
            }
        }

        // new : [1, 5] UPDATE
        // old : [1, 1] INSERT
        // new COVERS old
        newTree.reset();
        newTree.update(1L, 5L, i2);
        memoryData = newTree.queryMemory(null, null, null,  null);

        oldTree.reset();
        oldTree.insert(1L, i1);
        mergeAnswer = oldTree.queryMemory(null, null, null,   memoryData);
        Assert.assertEquals(mergeAnswer.valueLength, 2);
        for (int i = 0; i < mergeAnswer.valueLength; i++) {
            // outputDynamicOneColumn(mergeBlockAns, i);
            if (i == 0) {
                Assert.assertEquals(mergeAnswer.getTime(i * 2), 1);
                Assert.assertEquals(mergeAnswer.getTime(i * 2 + 1), -1);
                Assert.assertEquals(mergeAnswer.getInt(i), 2);
            } else if (i == 1) {
                Assert.assertEquals(mergeAnswer.getTime(i * 2), 2);
                Assert.assertEquals(mergeAnswer.getTime(i * 2 + 1), 5);
                Assert.assertEquals(mergeAnswer.getInt(i), 2);
            }
        }

        // new : [1, 5] UPDATE
        // old : [1, 5] UPDATE
        // new equals old
        newTree.reset();
        newTree.update(1L, 5L, i2);
        memoryData = newTree.queryMemory(null, null, null,  null);

        oldTree.reset();
        oldTree.update(1L, 5L, i1);
        mergeAnswer = oldTree.queryMemory(null, null, null, memoryData);
        Assert.assertEquals(mergeAnswer.valueLength, 1);
        for (int i = 0; i < mergeAnswer.valueLength; i++) {
            if (i == 0) {
                Assert.assertEquals(mergeAnswer.getTime(i * 2), 1);
                Assert.assertEquals(mergeAnswer.getTime(i * 2 + 1), 5);
                Assert.assertEquals(mergeAnswer.getInt(i), 2);
            }
        }

        // new : [2, 4] UPDATE
        // old : [1, 5] UPDATE
        // old covers new
        newTree.reset();
        newTree.update(2L, 4L, i2);
        memoryData = newTree.queryMemory(null, null, null,  null);

        oldTree.reset();
        oldTree.update(1L, 5L, i1);
        mergeAnswer = oldTree.queryMemory(null, null, null, memoryData);
        Assert.assertEquals(mergeAnswer.valueLength, 3);
        for (int i = 0; i < mergeAnswer.valueLength; i++) {
            // outputDynamicOneColumn(mergeBlockAns, i);
            if (i == 0) {
                Assert.assertEquals(mergeAnswer.getTime(i * 2), 1);
                Assert.assertEquals(mergeAnswer.getTime(i * 2 + 1), 1);
                Assert.assertEquals(mergeAnswer.getInt(i), 1);
            } else if (i == 1) {
                Assert.assertEquals(mergeAnswer.getTime(i * 2), 2);
                Assert.assertEquals(mergeAnswer.getTime(i * 2 + 1), 4);
                Assert.assertEquals(mergeAnswer.getInt(i), 2);
            } else if (i == 2) {
                Assert.assertEquals(mergeAnswer.getTime(i * 2), 5);
                Assert.assertEquals(mergeAnswer.getTime(i * 2 + 1), 5);
                Assert.assertEquals(mergeAnswer.getInt(i), 1);
            }
        }

        // new : [2, 4] UPDATE
        // old : [1, 5] [6, 7] UPDATE
        // old covers new
        newTree.reset();
        newTree.update(2L, 4L, i2);
        memoryData = newTree.queryMemory(null, null, null,  null);

        oldTree.reset();
        oldTree.update(1L, 5L, i1);
        oldTree.update(6L, 7L, i3);
        mergeAnswer = oldTree.queryMemory(null, null, null,  memoryData);
        Assert.assertEquals(mergeAnswer.valueLength, 4);
        for (int i = 0; i < mergeAnswer.valueLength; i++) {
            // outputDynamicOneColumn(mergeBlockAns, i);
            if (i == 0) {
                Assert.assertEquals(mergeAnswer.getTime(i * 2), 1);
                Assert.assertEquals(mergeAnswer.getTime(i * 2 + 1), 1);
                Assert.assertEquals(mergeAnswer.getInt(i), 1);
            } else if (i == 1) {
                Assert.assertEquals(mergeAnswer.getTime(i * 2), 2);
                Assert.assertEquals(mergeAnswer.getTime(i * 2 + 1), 4);
                Assert.assertEquals(mergeAnswer.getInt(i), 2);
            } else if (i == 2) {
                Assert.assertEquals(mergeAnswer.getTime(i * 2), 5);
                Assert.assertEquals(mergeAnswer.getTime(i * 2 + 1), 5);
                Assert.assertEquals(mergeAnswer.getInt(i), 1);
            } else if (i == 3) {
                Assert.assertEquals(mergeAnswer.getTime(i * 2), 6);
                Assert.assertEquals(mergeAnswer.getTime(i * 2 + 1), 7);
                Assert.assertEquals(mergeAnswer.getInt(i), 3);
            }
        }
    }
}
