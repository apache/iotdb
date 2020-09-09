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

package org.apache.iotdb.db.query.udf.datastructure;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.udf.api.iterator.RowRecordBatchIterator;
import org.apache.iotdb.db.query.udf.api.iterator.RowRecordIterator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.Binary;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ElasticSerializableRowRecordListTest extends SerializableListTest {

  private static final TSDataType[] DATA_TYPES = {TSDataType.INT32, TSDataType.INT64,
      TSDataType.FLOAT, TSDataType.DOUBLE, TSDataType.BOOLEAN, TSDataType.TEXT};

  private ElasticSerializableRowRecordList rowRecordList;

  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

  @After
  public void tearDown() {
    super.tearDown();
  }

  @Test
  public void testESRowRecordList() {
    initESRowRecordList();

    testPut();

    testOrderedAccessByIndex();

    testOrderedAccessByRowRecordIterator();

    testOrderedAccessBySizeLimitedRowRecordBatchIterator(0);
    testOrderedAccessBySizeLimitedRowRecordBatchIterator(ITERATION_TIMES / 2);

    testOrderedAccessByTimeWindowRowRecordBatchIterator(0, ITERATION_TIMES, BATCH_SIZE, BATCH_SIZE);
    testOrderedAccessByTimeWindowRowRecordBatchIterator((int) (0.25 * ITERATION_TIMES),
        (int) (0.75 * ITERATION_TIMES), BATCH_SIZE, BATCH_SIZE);

    testOrderedAccessByTimeWindowRowRecordBatchIterator(0, ITERATION_TIMES, BATCH_SIZE / 2,
        2 * BATCH_SIZE);
    testOrderedAccessByTimeWindowRowRecordBatchIterator((int) (0.25 * ITERATION_TIMES),
        (int) (0.75 * ITERATION_TIMES), BATCH_SIZE / 2, 2 * BATCH_SIZE);

    testOrderedAccessByTimeWindowRowRecordBatchIterator(0, ITERATION_TIMES, 2 * BATCH_SIZE,
        BATCH_SIZE / 2);
    testOrderedAccessByTimeWindowRowRecordBatchIterator((int) (0.25 * ITERATION_TIMES),
        (int) (0.75 * ITERATION_TIMES), 2 * BATCH_SIZE, BATCH_SIZE / 2);

    testOrderedAccessByTimeWindowRowRecordBatchIterator(0, 0, 2 * BATCH_SIZE, BATCH_SIZE / 2);

    testOrderedAccessByTimeWindowRowRecordBatchIterator((int) (0.25 * ITERATION_TIMES),
        (int) (0.6 * ITERATION_TIMES), 3 * BATCH_SIZE, (int) (0.3 * BATCH_SIZE));
    testOrderedAccessByTimeWindowRowRecordBatchIterator((int) (0.25 * ITERATION_TIMES),
        (int) (0.6 * ITERATION_TIMES), (int) (0.3 * BATCH_SIZE), 3 * BATCH_SIZE);
  }

  private void initESRowRecordList() {
    try {
      rowRecordList = new ElasticSerializableRowRecordList(DATA_TYPES, QUERY_ID, UNIQUE_ID,
          MEMORY_USAGE_LIMIT_IN_MB, CACHE_SIZE);
    } catch (QueryProcessException e) {
      fail(e.toString());
    }
    assertEquals(0, rowRecordList.size());
  }

  private void testPut() {
    try {
      for (int i = 0; i < ITERATION_TIMES; ++i) {
        RowRecord rowRecord = new RowRecord(i);
        for (TSDataType dataType : DATA_TYPES) {
          switch (dataType) {
            case INT32:
              rowRecord.addField(i, dataType);
              break;
            case INT64:
              rowRecord.addField((long) i, dataType);
              break;
            case FLOAT:
              rowRecord.addField((float) i, dataType);
              break;
            case DOUBLE:
              rowRecord.addField((double) i, dataType);
              break;
            case BOOLEAN:
              rowRecord.addField(i % 2 == 0, dataType);
              break;
            case TEXT:
              rowRecord.addField(Binary.valueOf(String.valueOf(i)), dataType);
              break;
          }
        }
        rowRecordList.put(rowRecord);
      }
    } catch (IOException e) {
      fail(e.toString());
    }
    assertEquals(ITERATION_TIMES, rowRecordList.size());
  }

  private void testRowRecord(RowRecord rowRecord, int expected) {
    List<Field> fields = rowRecord.getFields();
    for (int j = 0; j < DATA_TYPES.length; ++j) {
      switch (DATA_TYPES[j]) {
        case INT32:
          assertEquals(expected, fields.get(j).getIntV());
          break;
        case INT64:
          assertEquals(expected, fields.get(j).getLongV());
          break;
        case FLOAT:
          assertEquals(expected, fields.get(j).getFloatV(), 0);
          break;
        case DOUBLE:
          assertEquals(expected, fields.get(j).getDoubleV(), 0);
          break;
        case BOOLEAN:
          assertEquals(expected % 2 == 0, fields.get(j).getBoolV());
          break;
        case TEXT:
          assertEquals(Binary.valueOf(String.valueOf(expected)), fields.get(j).getBinaryV());
          break;
      }
    }
    assertEquals(DATA_TYPES.length, fields.size());
  }

  private void testOrderedAccessByIndex() {
    try {
      for (int i = 0; i < ITERATION_TIMES; ++i) {
        testRowRecord(rowRecordList.getRowRecord(i), i);
      }
    } catch (IOException e) {
      fail(e.toString());
    }
  }

  private void testOrderedAccessByRowRecordIterator() {
    RowRecordIterator iterator = rowRecordList.getRowRecordIterator();
    testOrderedAccessByRowRecordIteratorOnce(iterator);
    iterator.reset();
    testOrderedAccessByRowRecordIteratorOnce(iterator);
  }

  private void testOrderedAccessByRowRecordIteratorOnce(RowRecordIterator iterator) {
    int count = 0;
    try {
      while (iterator.hasNextRowRecord()) {
        assertEquals(count, iterator.nextTime());
        testRowRecord(iterator.nextRowRecord(), count);
        iterator.next();
        assertEquals(count, iterator.currentTime());
        testRowRecord(iterator.currentRowRecord(), count);
        ++count;
      }
    } catch (IOException e) {
      fail(e.toString());
    }
    assertEquals(ITERATION_TIMES, count);
  }

  private void testOrderedAccessBySizeLimitedRowRecordBatchIterator(int displayWindowBegin) {
    int iterationTimes = ITERATION_TIMES - displayWindowBegin;
    int totalInFirstExecution = 0;
    int totalInSecondExecution = 0;
    try {
      // test different constructors
      RowRecordBatchIterator batchIterator = iterationTimes == ITERATION_TIMES
          ? rowRecordList.getSizeLimitedBatchIterator(BATCH_SIZE)
          : rowRecordList.getSizeLimitedBatchIterator(BATCH_SIZE, displayWindowBegin);

      int batchCount = 0;
      while (batchIterator.hasNextBatch()) {
        batchIterator.next();
        testRandomAccessByIndexInRowRecordBatch(displayWindowBegin + totalInFirstExecution,
            batchIterator);
        totalInFirstExecution = testRowRecordIteratorGeneratedByRowRecordBatchIterator(
            displayWindowBegin + batchIterator.currentBatchIndex() * BATCH_SIZE,
            batchIterator.currentBatch(), BATCH_SIZE, totalInFirstExecution);
        ++batchCount;
      }
      assertEquals(iterationTimes / BATCH_SIZE, batchCount);

      batchIterator.reset();

      batchCount = 0;
      while (batchIterator.hasNextBatch()) {
        batchIterator.next();
        testRandomAccessByIndexInRowRecordBatch(displayWindowBegin + totalInSecondExecution,
            batchIterator);
        totalInSecondExecution = testRowRecordIteratorGeneratedByRowRecordBatchIterator(
            displayWindowBegin + batchIterator.currentBatchIndex() * BATCH_SIZE,
            batchIterator.currentBatch(), BATCH_SIZE, totalInSecondExecution);
        ++batchCount;
      }
      assertEquals(iterationTimes / BATCH_SIZE, batchCount);

      assertEquals(totalInFirstExecution, totalInSecondExecution);
    } catch (IOException | QueryProcessException e) {
      fail(e.toString());
    }
    assertEquals(iterationTimes, totalInFirstExecution);
  }

  private void testOrderedAccessByTimeWindowRowRecordBatchIterator(int displayWindowBegin,
      int displayWindowEnd, int timeInterval, int slidingStep) {
    int timeWindow = displayWindowEnd - displayWindowBegin;
    try {
      RowRecordBatchIterator batchIterator = timeWindow == ITERATION_TIMES
          ? rowRecordList.getTimeWindowBatchIterator(timeInterval, slidingStep)
          : rowRecordList.getTimeWindowBatchIterator(displayWindowBegin, displayWindowEnd,
              timeInterval, slidingStep); // test different constructors

      testOrderedAccessByTimeWindowRowRecordBatchIteratorOnce(displayWindowBegin, displayWindowEnd,
          timeInterval, slidingStep, batchIterator);
      batchIterator.reset();
      testOrderedAccessByTimeWindowRowRecordBatchIteratorOnce(displayWindowBegin, displayWindowEnd,
          timeInterval, slidingStep, batchIterator);
    } catch (IOException | QueryProcessException e) {
      fail(e.toString());
    }
  }

  private void testOrderedAccessByTimeWindowRowRecordBatchIteratorOnce(int displayWindowBegin,
      int displayWindowEnd, int timeInterval, int slidingStep, RowRecordBatchIterator batchIterator)
      throws IOException {
    int timeWindow = displayWindowEnd - displayWindowBegin;
    int expectedTotal = 0;
    int actualTotal = 0;
    int batchCount = 0;

    while (batchIterator.hasNextBatch()) {
      batchIterator.next();
      int initialIndex = displayWindowBegin + batchIterator.currentBatchIndex() * slidingStep;
      testRandomAccessByIndexInRowRecordBatch(initialIndex, batchIterator);
      int expectedBatchSize = displayWindowEnd < initialIndex + timeInterval
          ? displayWindowEnd - initialIndex : timeInterval;

      RowRecordIterator rowRecordIterator = batchIterator.currentBatch();
      int actualTotalInFirstExecution = testRowRecordIteratorGeneratedByRowRecordBatchIterator(
          initialIndex, rowRecordIterator, expectedBatchSize, actualTotal);
      rowRecordIterator.reset();
      actualTotal = testRowRecordIteratorGeneratedByRowRecordBatchIterator(initialIndex,
          rowRecordIterator, expectedBatchSize, actualTotal);
      assertEquals(actualTotalInFirstExecution, actualTotal);

      expectedTotal += expectedBatchSize;
      ++batchCount;
    }

    assertEquals((int) Math.ceil((float) timeWindow / slidingStep), batchCount);
    assertEquals(expectedTotal, actualTotal);
  }

  private int testRowRecordIteratorGeneratedByRowRecordBatchIterator(int initialIndex,
      RowRecordIterator rowRecordIterator, int expectedRowRecordCount, int total) {
    int totalInFirstExecution = testRowRecordIteratorGeneratedByRowRecordBatchIteratorOnce(
        initialIndex, rowRecordIterator, expectedRowRecordCount, total);
    rowRecordIterator.reset();
    total = testRowRecordIteratorGeneratedByRowRecordBatchIteratorOnce(initialIndex,
        rowRecordIterator, expectedRowRecordCount, total);
    assertEquals(totalInFirstExecution, total);
    return total;
  }

  private int testRowRecordIteratorGeneratedByRowRecordBatchIteratorOnce(int initialIndex,
      RowRecordIterator rowRecordIterator, int expectedRowRecordCount, int total) {
    int actualRowRecordCount = 0;
    try {
      while (rowRecordIterator.hasNextRowRecord()) {
        int expected = initialIndex + actualRowRecordCount;
        assertEquals(expected, rowRecordIterator.nextTime());
        testRowRecord(rowRecordIterator.nextRowRecord(), expected);
        rowRecordIterator.next();
        assertEquals(expected, rowRecordIterator.currentTime());
        testRowRecord(rowRecordIterator.currentRowRecord(), expected);
        ++total;
        ++actualRowRecordCount;
      }
      assertEquals(expectedRowRecordCount, actualRowRecordCount);
    } catch (IOException e) {
      fail(e.toString());
    }
    return total;
  }

  private void testRandomAccessByIndexInRowRecordBatch(int initialIndex,
      RowRecordBatchIterator batchIterator) {
    int batchSize = batchIterator.currentBatchSize();
    List<Integer> accessOrder = new ArrayList<>(batchSize);
    for (int i = 0; i < batchSize; ++i) {
      accessOrder.add(i);
    }
    Collections.shuffle(accessOrder);

    try {
      for (int i = 0; i < batchSize; ++i) {
        int accessIndex = accessOrder.get(i);
        int expected = initialIndex + accessIndex;
        assertEquals(expected, batchIterator.getTimeInCurrentBatch(accessIndex));
        testRowRecord(batchIterator.getRowRecordInCurrentBatch(accessIndex), expected);
      }
    } catch (IOException e) {
      fail(e.toString());
    }
  }
}
