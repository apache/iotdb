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
import org.apache.tsfile.utils.TsPrimitiveType.TsVector;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class AlignedPriorityMergeReaderTest {

  @Test
  public void testOverlappingFieldsAscending() throws IOException {
    assertOverlappingFields(new AlignedPriorityMergeReader(), 10);
  }

  @Test
  public void testOverlappingFieldsDescending() throws IOException {
    assertOverlappingFields(new AlignedDescPriorityMergeReader(), 30);
  }

  private void assertOverlappingFields(PriorityMergeReader reader, long firstTime)
      throws IOException {
    BatchData oldest = new BatchData(TSDataType.VECTOR);
    oldest.putVector(firstTime, new TsPrimitiveType[] {null, new TsInt(100)});
    oldest.putVector(20, new TsPrimitiveType[] {null, new TsInt(200)});
    BatchData middle = new BatchData(TSDataType.VECTOR);
    middle.putVector(20, new TsPrimitiveType[] {null, new TsInt(220)});
    BatchData newest = new BatchData(TSDataType.VECTOR);
    newest.putVector(firstTime, new TsPrimitiveType[] {new TsInt(110), null});
    newest.putVector(20, new TsPrimitiveType[] {new TsInt(120), null});

    reader.addReader(oldest.getBatchDataIterator(), new MergeReaderPriority(0, 1, 0, true), 20);
    reader.addReader(middle.getBatchDataIterator(), new MergeReaderPriority(0, 2, 0, false), 20);
    reader.addReader(newest.getBatchDataIterator(), new MergeReaderPriority(0, 3, 0, false), 20);

    assertMergedRow(reader.nextTimeValuePair(), firstTime, 110, 100);
    assertMergedRow(reader.nextTimeValuePair(), 20, 120, 220);
    assertFalse(reader.hasNextTimeValuePair());
  }

  @Test
  public void testSkipOlderReaderOnlyWhenNextRowIsComplete() throws IOException {
    TsBinary oldValue = new TsBinary(new Binary(new byte[] {1}));
    TsBinary newValue = new TsBinary(Binary.EMPTY_VALUE);
    BatchData older = new BatchData(TSDataType.VECTOR);
    BatchData newer = new BatchData(TSDataType.VECTOR);
    for (long time : new long[] {10, 20, 30}) {
      older.putVector(time, new TsPrimitiveType[] {null, oldValue});
    }
    newer.putVector(10, new TsPrimitiveType[] {new TsInt(10), newValue});
    newer.putVector(20, new TsPrimitiveType[] {new TsInt(20), new TsBinary()});
    newer.putVector(30, new TsPrimitiveType[] {new TsInt(30), newValue});

    AlignedPriorityMergeReader reader = new AlignedPriorityMergeReader();
    reader.addReader(older.getBatchDataIterator(), new MergeReaderPriority(0, 1, 0, true), 30);
    reader.addReader(newer.getBatchDataIterator(), new MergeReaderPriority(0, 2, 0, false), 30);

    assertEquals(newValue, reader.nextTimeValuePair().getValue().getVector()[1]);
    assertEquals(oldValue, reader.nextTimeValuePair().getValue().getVector()[1]);
    // The newer row at 30 is complete, so the older reader can already be discarded.
    assertEquals(1, reader.heap.size());
    assertEquals(newValue, reader.nextTimeValuePair().getValue().getVector()[1]);
    assertFalse(reader.hasNextTimeValuePair());
  }

  private void assertMergedRow(TimeValuePair row, long time, int x, int y) {
    assertEquals(time, row.getTimestamp());
    assertEquals(x, row.getValue().getVector()[0].getInt());
    assertEquals(y, row.getValue().getVector()[1].getInt());
  }

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
}
