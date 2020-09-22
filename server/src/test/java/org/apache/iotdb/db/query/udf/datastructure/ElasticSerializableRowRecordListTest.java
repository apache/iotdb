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
import org.apache.iotdb.db.query.udf.datastructure.iterator.RowRecordWindowIterator;
import org.apache.iotdb.db.query.udf.datastructure.iterator.RowRecordIterator;
import org.apache.iotdb.db.query.udf.datastructure.row.ElasticSerializableRowRecordList;
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

    testOrderedAccessByTumblingTimeWindowIterator(0);
    testOrderedAccessByTumblingTimeWindowIterator(ITERATION_TIMES / 2);

    testOrderedAccessBySlidingTimeWindowIterator(0, ITERATION_TIMES, BATCH_SIZE, BATCH_SIZE);
    testOrderedAccessBySlidingTimeWindowIterator((int) (0.25 * ITERATION_TIMES),
        (int) (0.75 * ITERATION_TIMES), BATCH_SIZE, BATCH_SIZE);

    testOrderedAccessBySlidingTimeWindowIterator(0, ITERATION_TIMES, BATCH_SIZE / 2,
        2 * BATCH_SIZE);
    testOrderedAccessBySlidingTimeWindowIterator((int) (0.25 * ITERATION_TIMES),
        (int) (0.75 * ITERATION_TIMES), BATCH_SIZE / 2, 2 * BATCH_SIZE);

    testOrderedAccessBySlidingTimeWindowIterator(0, ITERATION_TIMES, 2 * BATCH_SIZE,
        BATCH_SIZE / 2);
    testOrderedAccessBySlidingTimeWindowIterator((int) (0.25 * ITERATION_TIMES),
        (int) (0.75 * ITERATION_TIMES), 2 * BATCH_SIZE, BATCH_SIZE / 2);

    testOrderedAccessBySlidingTimeWindowIterator(0, 0, 2 * BATCH_SIZE, BATCH_SIZE / 2);

    testOrderedAccessBySlidingTimeWindowIterator((int) (0.25 * ITERATION_TIMES),
        (int) (0.6 * ITERATION_TIMES), 3 * BATCH_SIZE, (int) (0.3 * BATCH_SIZE));
    testOrderedAccessBySlidingTimeWindowIterator((int) (0.25 * ITERATION_TIMES),
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

  private void testOrderedAccessByTumblingTimeWindowIterator(int displayWindowBegin) {
    int iterationTimes = ITERATION_TIMES - displayWindowBegin;
    int totalInFirstExecution = 0;
    int totalInSecondExecution = 0;
    try {
      // test different constructors
      RowRecordWindowIterator windowIterator = iterationTimes == ITERATION_TIMES
          ? rowRecordList.getTumblingTimeWindowIterator(BATCH_SIZE)
          : rowRecordList.getTumblingTimeWindowIterator(BATCH_SIZE, displayWindowBegin);

      int windowCount = 0;
      while (windowIterator.hasNextWindow()) {
        windowIterator.next();
        testRandomAccessByIndexInRowRecordWindow(displayWindowBegin + totalInFirstExecution,
            windowIterator);
        totalInFirstExecution = testRowRecordIteratorGeneratedByRowRecordWindowIterator(
            displayWindowBegin + windowIterator.currentWindowIndex() * BATCH_SIZE,
            windowIterator.currentWindow(), BATCH_SIZE, totalInFirstExecution);
        ++windowCount;
      }
      assertEquals(iterationTimes / BATCH_SIZE, windowCount);

      windowIterator.reset();

      windowCount = 0;
      while (windowIterator.hasNextWindow()) {
        windowIterator.next();
        testRandomAccessByIndexInRowRecordWindow(displayWindowBegin + totalInSecondExecution,
            windowIterator);
        totalInSecondExecution = testRowRecordIteratorGeneratedByRowRecordWindowIterator(
            displayWindowBegin + windowIterator.currentWindowIndex() * BATCH_SIZE,
            windowIterator.currentWindow(), BATCH_SIZE, totalInSecondExecution);
        ++windowCount;
      }
      assertEquals(iterationTimes / BATCH_SIZE, windowCount);

      assertEquals(totalInFirstExecution, totalInSecondExecution);
    } catch (IOException | QueryProcessException e) {
      fail(e.toString());
    }
    assertEquals(iterationTimes, totalInFirstExecution);
  }

  private void testOrderedAccessBySlidingTimeWindowIterator(int displayWindowBegin,
      int displayWindowEnd, int timeInterval, int slidingStep) {
    int timeWindow = displayWindowEnd - displayWindowBegin;
    try {
      RowRecordWindowIterator windowIterator = timeWindow == ITERATION_TIMES
          ? rowRecordList.getSlidingTimeWindowIterator(timeInterval, slidingStep)
          : rowRecordList.getSlidingTimeWindowIterator(timeInterval, slidingStep,
              displayWindowBegin, displayWindowEnd); // test different constructors

      testOrderedAccessBySlidingTimeWindowIteratorOnce(displayWindowBegin, displayWindowEnd,
          timeInterval, slidingStep, windowIterator);
      windowIterator.reset();
      testOrderedAccessBySlidingTimeWindowIteratorOnce(displayWindowBegin, displayWindowEnd,
          timeInterval, slidingStep, windowIterator);
    } catch (IOException | QueryProcessException e) {
      fail(e.toString());
    }
  }

  private void testOrderedAccessBySlidingTimeWindowIteratorOnce(int displayWindowBegin,
      int displayWindowEnd, int timeInterval, int slidingStep,
      RowRecordWindowIterator windowIterator)
      throws IOException {
    int timeWindow = displayWindowEnd - displayWindowBegin;
    int expectedTotal = 0;
    int actualTotal = 0;
    int windowCount = 0;

    while (windowIterator.hasNextWindow()) {
      windowIterator.next();
      int initialIndex = displayWindowBegin + windowIterator.currentWindowIndex() * slidingStep;
      testRandomAccessByIndexInRowRecordWindow(initialIndex, windowIterator);
      int expectedWindowSize = displayWindowEnd < initialIndex + timeInterval
          ? displayWindowEnd - initialIndex : timeInterval;

      RowRecordIterator rowRecordIterator = windowIterator.currentWindow();
      int actualTotalInFirstExecution = testRowRecordIteratorGeneratedByRowRecordWindowIterator(
          initialIndex, rowRecordIterator, expectedWindowSize, actualTotal);
      rowRecordIterator.reset();
      actualTotal = testRowRecordIteratorGeneratedByRowRecordWindowIterator(initialIndex,
          rowRecordIterator, expectedWindowSize, actualTotal);
      assertEquals(actualTotalInFirstExecution, actualTotal);

      expectedTotal += expectedWindowSize;
      ++windowCount;
    }

    assertEquals((int) Math.ceil((float) timeWindow / slidingStep), windowCount);
    assertEquals(expectedTotal, actualTotal);
  }

  private int testRowRecordIteratorGeneratedByRowRecordWindowIterator(int initialIndex,
      RowRecordIterator rowRecordIterator, int expectedRowRecordCount, int total) {
    int totalInFirstExecution = testRowRecordIteratorGeneratedByRowRecordWindowIteratorOnce(
        initialIndex, rowRecordIterator, expectedRowRecordCount, total);
    rowRecordIterator.reset();
    total = testRowRecordIteratorGeneratedByRowRecordWindowIteratorOnce(initialIndex,
        rowRecordIterator, expectedRowRecordCount, total);
    assertEquals(totalInFirstExecution, total);
    return total;
  }

  private int testRowRecordIteratorGeneratedByRowRecordWindowIteratorOnce(int initialIndex,
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

  private void testRandomAccessByIndexInRowRecordWindow(int initialIndex,
      RowRecordWindowIterator windowIterator) {
    int windowSize = windowIterator.currentWindowSize();
    List<Integer> accessOrder = new ArrayList<>(windowSize);
    for (int i = 0; i < windowSize; ++i) {
      accessOrder.add(i);
    }
    Collections.shuffle(accessOrder);

    try {
      for (int i = 0; i < windowSize; ++i) {
        int accessIndex = accessOrder.get(i);
        int expected = initialIndex + accessIndex;
        assertEquals(expected, windowIterator.getTimeInCurrentWindow(accessIndex));
        testRowRecord(windowIterator.getRowRecordInCurrentWindow(accessIndex), expected);
      }
    } catch (IOException e) {
      fail(e.toString());
    }
  }
}
