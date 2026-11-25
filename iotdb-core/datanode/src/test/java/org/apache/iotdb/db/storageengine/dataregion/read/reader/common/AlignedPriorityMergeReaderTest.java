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

import org.apache.tsfile.read.TimeValuePair;
import org.apache.tsfile.read.reader.IPointReader;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.TsPrimitiveType;
import org.apache.tsfile.utils.TsPrimitiveType.TsBinary;
import org.apache.tsfile.utils.TsPrimitiveType.TsInt;
import org.apache.tsfile.utils.TsPrimitiveType.TsLong;
import org.apache.tsfile.utils.TsPrimitiveType.TsVector;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

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

  /**
   * Regression test for issue #19392 (ascending scan): merging t=10 must not eagerly fill t=20's
   * null y with the oldest reader's stale value before the middle-priority reader at t=20 is seen.
   */
  @Test
  public void testAscScanMergesMiddlePriorityReaderAtNextTimestamp() throws IOException {
    assertMiddlePriorityMerge(new AlignedPriorityMergeReader(), 10L, 20L);
  }

  /** Symmetric regression test for descending aligned scans. */
  @Test
  public void testDescScanMergesMiddlePriorityReaderAtNextTimestamp() throws IOException {
    assertMiddlePriorityMerge(new AlignedDescPriorityMergeReader(), 20L, 10L);
  }

  private void assertMiddlePriorityMerge(
      PriorityMergeReader reader, long firstTime, long secondTime) throws IOException {
    addRows(
        reader,
        1,
        secondTime,
        alignedRow(firstTime, null, 100L),
        alignedRow(secondTime, null, 200L));
    addRows(reader, 2, secondTime, alignedRow(secondTime, null, 220L));
    addRows(
        reader,
        3,
        secondTime,
        alignedRow(firstTime, 110L, null),
        alignedRow(secondTime, 120L, null));

    assertRow(reader.nextTimeValuePair(), firstTime, 110L, 100L);
    assertRow(reader.nextTimeValuePair(), secondTime, 120L, 220L);
    assertFalse(reader.hasNextTimeValuePair());
    assertEquals(0L, reader.getUsedMemorySize());
  }

  /**
   * Verifies that when three readers contribute distinct columns at t=20, each column takes the
   * highest-priority non-null value (x from newest, y from middle, z from oldest).
   */
  @Test
  public void testAscScanPreservesOlderColumnsWhileMiddleOverridesOverlappingColumn()
      throws IOException {
    AlignedPriorityMergeReader reader = new AlignedPriorityMergeReader();
    addRows(reader, 1, 20L, alignedRow(10, null, 100L, 1000L), alignedRow(20, null, 200L, 2000L));
    addRows(reader, 2, 20L, alignedRow(20, null, 220L, null));
    addRows(reader, 3, 20L, alignedRow(10, 110L, null, null), alignedRow(20, 120L, null, null));

    assertRow(reader.nextTimeValuePair(), 10L, 110L, 100L, 1000L);
    assertRow(reader.nextTimeValuePair(), 20L, 120L, 220L, 2000L);
    assertFalse(reader.hasNextTimeValuePair());
    assertEquals(0L, reader.getUsedMemorySize());
  }

  /**
   * Verifies that aligned readers preserve the look-ahead skip optimization when {@code topNext}
   * has no null fields, while keeping older readers when a field is null (including {@link
   * TsBinary} with a null value), and that {@code fillNullValueInAligned} reports completion.
   */
  @Test
  public void testSkipOlderReaderOnlyWhenNextAlignedRowIsComplete() throws IOException {
    assertFalse(
        AlignedPriorityMergeReader.fillNullValueInAligned(
            alignedRow(1, 10L, null, null), alignedRow(1, null, 20L, null)));
    assertTrue(
        AlignedPriorityMergeReader.fillNullValueInAligned(
            alignedRow(1, 10L, null), alignedRow(1, null, 20L)));

    assertCompleteRowSkip(new AlignedPriorityMergeReader(), 10L, 20L, 30L);
    assertCompleteRowSkip(new AlignedDescPriorityMergeReader(), 30L, 20L, 10L);
  }

  private void assertCompleteRowSkip(PriorityMergeReader reader, long t1, long t2, long t3)
      throws IOException {
    TsBinary oldVal = new TsBinary(new Binary(new byte[] {1}));
    TsBinary emptyVal = new TsBinary(Binary.EMPTY_VALUE);

    addRows(reader, 1, t3, row(t1, null, oldVal), row(t2, null, oldVal), row(t3, null, oldVal));
    addRows(
        reader,
        2,
        t3,
        row(t1, new TsLong(10L), emptyVal),
        row(t2, new TsLong(20L), new TsBinary()),
        row(t3, new TsLong(30L), emptyVal));

    // topNext (t2) has a null-valued TsBinary, so the older reader must stay in the heap.
    assertBinaryRow(reader.nextTimeValuePair(), t1, 10L, emptyVal);
    assertEquals(2, reader.heap.size());

    // topNext (t3) is complete, so the older reader's final point at t3 is skipped immediately.
    assertBinaryRow(reader.nextTimeValuePair(), t2, 20L, oldVal);
    assertEquals(1, reader.heap.size());

    assertBinaryRow(reader.nextTimeValuePair(), t3, 30L, emptyVal);
    assertFalse(reader.hasNextTimeValuePair());
    assertEquals(0L, reader.getUsedMemorySize());
  }

  private static void addRows(
      PriorityMergeReader reader, int priority, long endTime, TimeValuePair... rows)
      throws IOException {
    reader.addReader(
        new ListAlignedPointReader(Arrays.asList(rows)),
        new MergeReaderPriority(priority, 1, 0, false),
        endTime);
  }

  private static void assertRow(TimeValuePair row, long expectedTime, long... expectedValues) {
    assertEquals(expectedTime, row.getTimestamp());
    TsPrimitiveType[] vec = row.getValue().getVector();
    for (int i = 0; i < expectedValues.length; i++) {
      assertEquals(expectedValues[i], vec[i].getLong());
    }
  }

  private static void assertBinaryRow(
      TimeValuePair row, long expectedTime, long expectedX, TsBinary expectedY) {
    assertEquals(expectedTime, row.getTimestamp());
    assertEquals(expectedX, row.getValue().getVector()[0].getLong());
    assertEquals(expectedY, row.getValue().getVector()[1]);
  }

  private static TimeValuePair row(long timestamp, TsPrimitiveType... fields) {
    return new TimeValuePair(timestamp, new TsVector(fields));
  }

  private static TimeValuePair alignedRow(long timestamp, Long... values) {
    TsPrimitiveType[] vector = new TsPrimitiveType[values.length];
    for (int i = 0; i < values.length; i++) {
      if (values[i] != null) {
        vector[i] = new TsLong(values[i]);
      }
    }
    return new TimeValuePair(timestamp, new TsVector(vector));
  }

  private static class ListAlignedPointReader implements IPointReader {

    private final List<TimeValuePair> points;
    private int index = 0;

    ListAlignedPointReader(List<TimeValuePair> points) {
      this.points = points;
    }

    @Override
    public boolean hasNextTimeValuePair() {
      return index < points.size();
    }

    @Override
    public TimeValuePair nextTimeValuePair() {
      return points.get(index++);
    }

    @Override
    public TimeValuePair currentTimeValuePair() {
      return points.get(index);
    }

    @Override
    public long getUsedMemorySize() {
      return points.size();
    }

    @Override
    public void close() {}
  }
}
