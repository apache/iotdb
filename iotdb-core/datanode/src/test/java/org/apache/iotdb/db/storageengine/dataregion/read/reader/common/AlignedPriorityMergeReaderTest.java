/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.storageengine.dataregion.read.reader.common;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.utils.TsPrimitiveType.TsBinary;
import org.apache.tsfile.utils.TsPrimitiveType.TsInt;
import org.apache.tsfile.utils.TsPrimitiveType.TsLong;
import org.apache.tsfile.utils.TsPrimitiveType.TsVector;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AlignedPriorityMergeReaderTest {

  /**
   * TimeValuePair with high priority needs to be filled [10, null, null] TimeValuePair with low
   * priority is used to fill the above [null, 2, 3] expected result: [10, 2, 3]
   */
  @Test
  public void test1() {
    TsPrimitiveType[] vector = new TsPrimitiveType[3];
    vector[0] = new TsInt(10);
    TimeValuePair v = new TimeValuePair(1, new TsVector(vector));
    vector = new TsPrimitiveType[3];
    vector[1] = new TsInt(2);
    vector[2] = new TsInt(3);
    TimeValuePair c = new TimeValuePair(1, new TsVector(vector));
    AlignedPriorityMergeReader.fillNullValueInAligned(v, c);
    assertEquals(10, v.getValue().getVector()[0].getInt());
    assertEquals(2, v.getValue().getVector()[1].getInt());
    assertEquals(3, v.getValue().getVector()[2].getInt());
  }

  /**
   * TimeValuePair with high priority needs to be filled [10, 20, null] TimeValuePair with low
   * priority is used to fill the above [null, 2, 3] expected result: [10, 20, 3]
   */
  @Test
  public void test2() {
    TsPrimitiveType[] vector = new TsPrimitiveType[3];
    vector[0] = new TsInt(10);
    vector[1] = new TsInt(20);
    TimeValuePair v = new TimeValuePair(1, new TsVector(vector));
    vector = new TsPrimitiveType[3];
    vector[1] = new TsInt(2);
    vector[2] = new TsInt(3);
    TimeValuePair c = new TimeValuePair(1, new TsVector(vector));
    AlignedPriorityMergeReader.fillNullValueInAligned(v, c);
    assertEquals(10, v.getValue().getVector()[0].getInt());
    assertEquals(20, v.getValue().getVector()[1].getInt());
    assertEquals(3, v.getValue().getVector()[2].getInt());
  }

  /**
   * TimeValuePair with high priority needs to be filled [10, 20, 30] TimeValuePair with low
   * priority is used to fill the above [null, 2, 3] expected result: [10, 20, 30]
   */
  @Test
  public void test3() {
    TsPrimitiveType[] vector = new TsPrimitiveType[3];
    vector[0] = new TsInt(10);
    vector[1] = new TsInt(20);
    vector[2] = new TsInt(30);
    TimeValuePair v = new TimeValuePair(1, new TsVector(vector));
    vector = new TsPrimitiveType[3];
    vector[1] = new TsInt(2);
    vector[2] = new TsInt(3);
    TimeValuePair c = new TimeValuePair(1, new TsVector(vector));
    AlignedPriorityMergeReader.fillNullValueInAligned(v, c);
    assertEquals(10, v.getValue().getVector()[0].getInt());
    assertEquals(20, v.getValue().getVector()[1].getInt());
    assertEquals(30, v.getValue().getVector()[2].getInt());
  }

  @Test
  public void testAscScanMergesMiddlePriorityReaderAtNextTimestamp() throws IOException {
    assertMiddlePriorityMerge(new AlignedPriorityMergeReader(), 10L, 20L);
  }

  @Test
  public void testDescScanMergesMiddlePriorityReaderAtNextTimestamp() throws IOException {
    assertMiddlePriorityMerge(new AlignedDescPriorityMergeReader(), 20L, 10L);
  }

  private void assertMiddlePriorityMerge(PriorityMergeReader reader, long t1, long t2)
      throws IOException {
    addBatch(reader, 1, t2, batch(t1, vec(null, 100L), t2, vec(null, 200L)));
    addBatch(reader, 2, t2, batch(t2, vec(null, 220L)));
    addBatch(reader, 3, t2, batch(t1, vec(110L, null), t2, vec(120L, null)));

    assertLongRow(reader.nextTimeValuePair(), t1, 110L, 100L);
    assertLongRow(reader.nextTimeValuePair(), t2, 120L, 220L);
    assertFalse(reader.hasNextTimeValuePair());
  }

  @Test
  public void testAscScanPreservesOlderColumnsWhileMiddleOverridesOverlappingColumn()
      throws IOException {
    AlignedPriorityMergeReader reader = new AlignedPriorityMergeReader();
    addBatch(reader, 1, 20L, batch(10L, vec(null, 100L, 1000L), 20L, vec(null, 200L, 2000L)));
    addBatch(reader, 2, 20L, batch(20L, vec(null, 220L, null)));
    addBatch(reader, 3, 20L, batch(10L, vec(110L, null, null), 20L, vec(120L, null, null)));

    assertLongRow(reader.nextTimeValuePair(), 10L, 110L, 100L, 1000L);
    assertLongRow(reader.nextTimeValuePair(), 20L, 120L, 220L, 2000L);
    assertFalse(reader.hasNextTimeValuePair());
  }

  @Test
  public void testSkipOlderReaderOnlyWhenNextAlignedRowIsComplete() throws IOException {
    assertFalse(
        AlignedPriorityMergeReader.fillNullValueInAligned(
            new TimeValuePair(1, new TsVector(vec(10L, null, null))),
            new TimeValuePair(1, new TsVector(vec(null, 20L, null)))));
    assertTrue(
        AlignedPriorityMergeReader.fillNullValueInAligned(
            new TimeValuePair(1, new TsVector(vec(10L, null))),
            new TimeValuePair(1, new TsVector(vec(null, 20L)))));

    assertCompleteRowSkip(new AlignedPriorityMergeReader(), 10L, 20L, 30L);
    assertCompleteRowSkip(new AlignedDescPriorityMergeReader(), 30L, 20L, 10L);
  }

  private void assertCompleteRowSkip(PriorityMergeReader reader, long t1, long t2, long t3)
      throws IOException {
    TsBinary oldVal = new TsBinary(new Binary(new byte[] {1}));
    TsBinary emptyVal = new TsBinary(Binary.EMPTY_VALUE);

    BatchData older = new BatchData(TSDataType.VECTOR);
    for (long t : new long[] {t1, t2, t3}) {
      older.putVector(t, new TsPrimitiveType[] {null, oldVal});
    }
    BatchData newer = new BatchData(TSDataType.VECTOR);
    newer.putVector(t1, new TsPrimitiveType[] {new TsLong(10L), emptyVal});
    newer.putVector(t2, new TsPrimitiveType[] {new TsLong(20L), new TsBinary()});
    newer.putVector(t3, new TsPrimitiveType[] {new TsLong(30L), emptyVal});

    addBatch(reader, 1, t3, older);
    addBatch(reader, 2, t3, newer);

    // topNext (t2) has a null-valued TsBinary, so older stays in the heap; at t2, topNext (t3) is
    // complete, so older is skipped and closed immediately.
    assertEquals(emptyVal, reader.nextTimeValuePair().getValue().getVector()[1]);
    assertEquals(2, reader.heap.size());
    assertEquals(oldVal, reader.nextTimeValuePair().getValue().getVector()[1]);
    assertEquals(1, reader.heap.size());
    assertEquals(emptyVal, reader.nextTimeValuePair().getValue().getVector()[1]);
    assertFalse(reader.hasNextTimeValuePair());
  }

  private static void addBatch(PriorityMergeReader reader, int prio, long endTime, BatchData data)
      throws IOException {
    reader.addReader(
        data.getBatchDataIterator(), new MergeReaderPriority(prio, 1, 0, false), endTime);
  }

  private static BatchData batch(long t1, TsPrimitiveType[] v1) {
    BatchData b = new BatchData(TSDataType.VECTOR);
    b.putVector(t1, v1);
    return b;
  }

  private static BatchData batch(long t1, TsPrimitiveType[] v1, long t2, TsPrimitiveType[] v2) {
    BatchData b = batch(t1, v1);
    b.putVector(t2, v2);
    return b;
  }

  private static TsPrimitiveType[] vec(Long... values) {
    TsPrimitiveType[] v = new TsPrimitiveType[values.length];
    for (int i = 0; i < values.length; i++) {
      if (values[i] != null) {
        v[i] = new TsLong(values[i]);
      }
    }
    return v;
  }

  private static void assertLongRow(TimeValuePair row, long expectedTime, long... expectedValues) {
    assertEquals(expectedTime, row.getTimestamp());
    for (int i = 0; i < expectedValues.length; i++) {
      assertEquals(expectedValues[i], row.getValue().getVector()[i].getLong());
    }
  }
}
