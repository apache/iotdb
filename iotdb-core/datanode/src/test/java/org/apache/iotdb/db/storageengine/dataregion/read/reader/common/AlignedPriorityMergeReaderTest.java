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
   * Regression test for issue #19392 (ascending scan): oldest and newest readers both have rows at
   * t=10 and t=20, while a middle-priority reader starts at t=20 with a newer value for y. Merging
   * t=10 must not eagerly fill t=20's null y with the oldest reader's stale value.
   */
  @Test
  public void testAscScanMergesMiddlePriorityReaderAtNextTimestamp() throws IOException {
    AlignedPriorityMergeReader reader = new AlignedPriorityMergeReader();
    reader.addReader(
        new ListAlignedPointReader(
            Arrays.asList(alignedRow(10, null, 100L), alignedRow(20, null, 200L))),
        new MergeReaderPriority(1, 1, 0, false),
        20);
    reader.addReader(
        new ListAlignedPointReader(Arrays.asList(alignedRow(20, null, 220L))),
        new MergeReaderPriority(2, 1, 0, false),
        20);
    reader.addReader(
        new ListAlignedPointReader(
            Arrays.asList(alignedRow(10, 110L, null), alignedRow(20, 120L, null))),
        new MergeReaderPriority(3, 1, 0, false),
        20);

    assertTrue(reader.hasNextTimeValuePair());
    TimeValuePair row1 = reader.nextTimeValuePair();
    assertEquals(10L, row1.getTimestamp());
    assertEquals(110L, row1.getValue().getVector()[0].getLong());
    assertEquals(100L, row1.getValue().getVector()[1].getLong());

    assertTrue(reader.hasNextTimeValuePair());
    TimeValuePair row2 = reader.nextTimeValuePair();
    assertEquals(20L, row2.getTimestamp());
    assertEquals(120L, row2.getValue().getVector()[0].getLong());
    assertEquals(220L, row2.getValue().getVector()[1].getLong());

    assertFalse(reader.hasNextTimeValuePair());
    assertEquals(0L, reader.getUsedMemorySize());
  }

  /**
   * Symmetric regression test for descending aligned scans: processing t=20 must not eagerly fill
   * t=10's null y with the oldest reader's stale value before the middle-priority reader at t=10 is
   * visited.
   */
  @Test
  public void testDescScanMergesMiddlePriorityReaderAtNextTimestamp() throws IOException {
    AlignedDescPriorityMergeReader reader = new AlignedDescPriorityMergeReader();
    reader.addReader(
        new ListAlignedPointReader(
            Arrays.asList(alignedRow(20, null, 200L), alignedRow(10, null, 100L))),
        new MergeReaderPriority(1, 1, 0, false),
        10);
    reader.addReader(
        new ListAlignedPointReader(Arrays.asList(alignedRow(10, null, 120L))),
        new MergeReaderPriority(2, 1, 0, false),
        10);
    reader.addReader(
        new ListAlignedPointReader(
            Arrays.asList(alignedRow(20, 210L, null), alignedRow(10, 110L, null))),
        new MergeReaderPriority(3, 1, 0, false),
        10);

    assertTrue(reader.hasNextTimeValuePair());
    TimeValuePair row1 = reader.nextTimeValuePair();
    assertEquals(20L, row1.getTimestamp());
    assertEquals(210L, row1.getValue().getVector()[0].getLong());
    assertEquals(200L, row1.getValue().getVector()[1].getLong());

    assertTrue(reader.hasNextTimeValuePair());
    TimeValuePair row2 = reader.nextTimeValuePair();
    assertEquals(10L, row2.getTimestamp());
    assertEquals(110L, row2.getValue().getVector()[0].getLong());
    assertEquals(120L, row2.getValue().getVector()[1].getLong());

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
    reader.addReader(
        new ListAlignedPointReader(
            Arrays.asList(alignedRow(10, null, 100L, 1000L), alignedRow(20, null, 200L, 2000L))),
        new MergeReaderPriority(1, 1, 0, false),
        20);
    reader.addReader(
        new ListAlignedPointReader(Arrays.asList(alignedRow(20, null, 220L, null))),
        new MergeReaderPriority(2, 1, 0, false),
        20);
    reader.addReader(
        new ListAlignedPointReader(
            Arrays.asList(alignedRow(10, 110L, null, null), alignedRow(20, 120L, null, null))),
        new MergeReaderPriority(3, 1, 0, false),
        20);

    TimeValuePair row1 = reader.nextTimeValuePair();
    assertEquals(10L, row1.getTimestamp());
    assertEquals(110L, row1.getValue().getVector()[0].getLong());
    assertEquals(100L, row1.getValue().getVector()[1].getLong());
    assertEquals(1000L, row1.getValue().getVector()[2].getLong());

    TimeValuePair row2 = reader.nextTimeValuePair();
    assertEquals(20L, row2.getTimestamp());
    assertEquals(120L, row2.getValue().getVector()[0].getLong());
    assertEquals(220L, row2.getValue().getVector()[1].getLong());
    assertEquals(2000L, row2.getValue().getVector()[2].getLong());

    assertFalse(reader.hasNextTimeValuePair());
    assertEquals(0L, reader.getUsedMemorySize());
  }

  /**
   * Verifies that aligned readers (both ascending and descending) preserve the look-ahead skip
   * optimization when {@code topNext} has no null fields, while keeping older readers in the heap
   * whenever {@code topNext} has a null field (including a {@link TsBinary} with a null value).
   */
  @Test
  public void testSkipOlderReaderOnlyWhenNextAlignedRowIsComplete() throws IOException {
    assertCompleteRowSkip(new AlignedPriorityMergeReader(), 10L, 20L, 30L);
    assertCompleteRowSkip(new AlignedDescPriorityMergeReader(), 30L, 20L, 10L);
  }

  private void assertCompleteRowSkip(PriorityMergeReader reader, long t1, long t2, long t3)
      throws IOException {
    TsBinary oldVal = new TsBinary(new Binary(new byte[] {1}));
    TsBinary emptyVal = new TsBinary(Binary.EMPTY_VALUE);

    reader.addReader(
        new ListAlignedPointReader(
            Arrays.asList(
                new TimeValuePair(t1, new TsVector(new TsPrimitiveType[] {null, oldVal})),
                new TimeValuePair(t2, new TsVector(new TsPrimitiveType[] {null, oldVal})),
                new TimeValuePair(t3, new TsVector(new TsPrimitiveType[] {null, oldVal})))),
        new MergeReaderPriority(1, 1, 0, false),
        t3);
    reader.addReader(
        new ListAlignedPointReader(
            Arrays.asList(
                new TimeValuePair(
                    t1, new TsVector(new TsPrimitiveType[] {new TsLong(10L), emptyVal})),
                new TimeValuePair(
                    t2, new TsVector(new TsPrimitiveType[] {new TsLong(20L), new TsBinary()})),
                new TimeValuePair(
                    t3, new TsVector(new TsPrimitiveType[] {new TsLong(30L), emptyVal})))),
        new MergeReaderPriority(2, 1, 0, false),
        t3);

    // At t1, topNext (t2) has a null-valued TsBinary, so the older reader must stay in the heap.
    TimeValuePair row1 = reader.nextTimeValuePair();
    assertEquals(t1, row1.getTimestamp());
    assertEquals(10L, row1.getValue().getVector()[0].getLong());
    assertEquals(emptyVal, row1.getValue().getVector()[1]);
    assertEquals(2, reader.heap.size());

    // At t2, the older reader fills t2's null binary field; topNext (t3) is complete, so the older
    // reader's final point at t3 is skipped and closed immediately.
    TimeValuePair row2 = reader.nextTimeValuePair();
    assertEquals(t2, row2.getTimestamp());
    assertEquals(20L, row2.getValue().getVector()[0].getLong());
    assertEquals(oldVal, row2.getValue().getVector()[1]);
    assertEquals(1, reader.heap.size());

    TimeValuePair row3 = reader.nextTimeValuePair();
    assertEquals(t3, row3.getTimestamp());
    assertEquals(30L, row3.getValue().getVector()[0].getLong());
    assertEquals(emptyVal, row3.getValue().getVector()[1]);

    assertFalse(reader.hasNextTimeValuePair());
    assertEquals(0L, reader.getUsedMemorySize());
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
