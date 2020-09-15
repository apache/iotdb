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

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.udf.api.iterator.DataPointWindowIterator;
import org.apache.iotdb.db.query.udf.api.iterator.DataPointIterator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ElasticSerializableTVListTest extends SerializableListTest {

  private ElasticSerializableTVList tvList;

  @Before
  public void setUp() throws Exception {
    super.setUp();
  }

  @After
  public void tearDown() {
    super.tearDown();
  }

  @Test
  public void testESIntTVList() {
    testESTVList(TSDataType.INT32);
  }

  @Test
  public void testESLongTVList() {
    testESTVList(TSDataType.INT64);
  }

  @Test
  public void testESFloatTVList() {
    testESTVList(TSDataType.FLOAT);
  }

  @Test
  public void testESDoubleTVList() {
    testESTVList(TSDataType.DOUBLE);
  }

  @Test
  public void testESTextTVList() {
    testESTVList(TSDataType.TEXT);
  }

  @Test
  public void testESBooleanTVList() {
    testESTVList(TSDataType.BOOLEAN);
  }

  private void testESTVList(TSDataType dataType) {
    initESTVList(dataType);

    testPut(dataType);

    testOrderedAccessByIndex(dataType);

    testOrderedAccessByDataPointIterator(dataType);

    testOrderedAccessByTumblingTimeWindowIterator(dataType, 0);
    testOrderedAccessByTumblingTimeWindowIterator(dataType, ITERATION_TIMES / 2);

    testOrderedAccessBySlidingTimeWindowIterator(dataType, 0, ITERATION_TIMES, BATCH_SIZE,
        BATCH_SIZE);
    testOrderedAccessBySlidingTimeWindowIterator(dataType, (int) (0.25 * ITERATION_TIMES),
        (int) (0.75 * ITERATION_TIMES), BATCH_SIZE, BATCH_SIZE);

    testOrderedAccessBySlidingTimeWindowIterator(dataType, 0, ITERATION_TIMES,
        BATCH_SIZE / 2, 2 * BATCH_SIZE);
    testOrderedAccessBySlidingTimeWindowIterator(dataType, (int) (0.25 * ITERATION_TIMES),
        (int) (0.75 * ITERATION_TIMES), BATCH_SIZE / 2, 2 * BATCH_SIZE);

    testOrderedAccessBySlidingTimeWindowIterator(dataType, 0, ITERATION_TIMES,
        2 * BATCH_SIZE, BATCH_SIZE / 2);
    testOrderedAccessBySlidingTimeWindowIterator(dataType, (int) (0.25 * ITERATION_TIMES),
        (int) (0.75 * ITERATION_TIMES), 2 * BATCH_SIZE, BATCH_SIZE / 2);

    testOrderedAccessBySlidingTimeWindowIterator(dataType, 0, 0, 2 * BATCH_SIZE,
        BATCH_SIZE / 2);

    testOrderedAccessBySlidingTimeWindowIterator(dataType, (int) (0.25 * ITERATION_TIMES),
        (int) (0.6 * ITERATION_TIMES), 3 * BATCH_SIZE, (int) (0.3 * BATCH_SIZE));
    testOrderedAccessBySlidingTimeWindowIterator(dataType, (int) (0.25 * ITERATION_TIMES),
        (int) (0.6 * ITERATION_TIMES), (int) (0.3 * BATCH_SIZE), 3 * BATCH_SIZE);
  }

  private void initESTVList(TSDataType dataType) {
    try {
      tvList = new ElasticSerializableTVList(dataType, QUERY_ID, UNIQUE_ID,
          MEMORY_USAGE_LIMIT_IN_MB, CACHE_SIZE);
    } catch (QueryProcessException e) {
      fail(e.toString());
    }
    assertEquals(0, tvList.size());
  }

  private void testPut(TSDataType dataType) {
    try {
      switch (dataType) {
        case INT32:
          for (int i = 0; i < ITERATION_TIMES; ++i) {
            tvList.putInt(i, i);
          }
          break;
        case INT64:
          for (int i = 0; i < ITERATION_TIMES; ++i) {
            tvList.putLong(i, i);
          }
          break;
        case FLOAT:
          for (int i = 0; i < ITERATION_TIMES; ++i) {
            tvList.putFloat(i, i);
          }
          break;
        case DOUBLE:
          for (int i = 0; i < ITERATION_TIMES; ++i) {
            tvList.putDouble(i, i);
          }
          break;
        case BOOLEAN:
          for (int i = 0; i < ITERATION_TIMES; ++i) {
            tvList.putBoolean(i, i % 2 == 0);
          }
          break;
        case TEXT:
          for (int i = 0; i < ITERATION_TIMES; ++i) {
            tvList.putBinary(i, Binary.valueOf(String.valueOf(i)));
          }
          break;
      }
    } catch (IOException e) {
      fail(e.toString());
    }
    assertEquals(ITERATION_TIMES, tvList.size());
  }

  private void testOrderedAccessByIndex(TSDataType dataType) {
    try {
      switch (dataType) {
        case INT32:
          for (int i = 0; i < ITERATION_TIMES; ++i) {
            assertEquals(i, tvList.getTime(i));
            assertEquals(i, tvList.getInt(i));
          }
          break;
        case INT64:
          for (int i = 0; i < ITERATION_TIMES; ++i) {
            assertEquals(i, tvList.getTime(i));
            assertEquals(i, tvList.getLong(i));
          }
          break;
        case FLOAT:
          for (int i = 0; i < ITERATION_TIMES; ++i) {
            assertEquals(i, tvList.getTime(i));
            assertEquals(i, tvList.getFloat(i), 0);
          }
          break;
        case DOUBLE:
          for (int i = 0; i < ITERATION_TIMES; ++i) {
            assertEquals(i, tvList.getTime(i));
            assertEquals(i, tvList.getDouble(i), 0);
          }
          break;
        case BOOLEAN:
          for (int i = 0; i < ITERATION_TIMES; ++i) {
            assertEquals(i, tvList.getTime(i));
            assertEquals(i % 2 == 0, tvList.getBoolean(i));
          }
          break;
        case TEXT:
          for (int i = 0; i < ITERATION_TIMES; ++i) {
            assertEquals(i, tvList.getTime(i));
            assertEquals(Binary.valueOf(String.valueOf(i)), tvList.getBinary(i));
          }
          break;
      }
    } catch (IOException e) {
      fail(e.toString());
    }
  }

  private void testOrderedAccessByDataPointIterator(TSDataType dataType) {
    DataPointIterator iterator = tvList.getDataPointIterator();
    testOrderedAccessByDataPointIteratorOnce(dataType, iterator);
    iterator.reset();
    testOrderedAccessByDataPointIteratorOnce(dataType, iterator);
  }

  private void testOrderedAccessByDataPointIteratorOnce(TSDataType dataType,
      DataPointIterator iterator) {
    int count = 0;
    try {
      switch (dataType) {
        case INT32:
          while (iterator.hasNextPoint()) {
            assertEquals(count, iterator.nextTime());
            assertEquals(count, iterator.nextInt());
            iterator.next();
            assertEquals(count, iterator.currentTime());
            assertEquals(count, iterator.currentInt());
            ++count;
          }
          break;
        case INT64:
          while (iterator.hasNextPoint()) {
            assertEquals(count, iterator.nextTime());
            assertEquals(count, iterator.nextLong());
            iterator.next();
            assertEquals(count, iterator.currentTime());
            assertEquals(count, iterator.currentLong());
            ++count;
          }
          break;
        case FLOAT:
          while (iterator.hasNextPoint()) {
            assertEquals(count, iterator.nextTime());
            assertEquals(count, iterator.nextFloat(), 0);
            iterator.next();
            assertEquals(count, iterator.currentTime());
            assertEquals(count, iterator.currentFloat(), 0);
            ++count;
          }
          break;
        case DOUBLE:
          while (iterator.hasNextPoint()) {
            assertEquals(count, iterator.nextTime());
            assertEquals(count, iterator.nextDouble(), 0);
            iterator.next();
            assertEquals(count, iterator.currentTime());
            assertEquals(count, iterator.currentDouble(), 0);
            ++count;
          }
          break;
        case BOOLEAN:
          while (iterator.hasNextPoint()) {
            assertEquals(count, iterator.nextTime());
            assertEquals(count % 2 == 0, iterator.nextBoolean());
            iterator.next();
            assertEquals(count, iterator.currentTime());
            assertEquals(count % 2 == 0, iterator.currentBoolean());
            ++count;
          }
          break;
        case TEXT:
          while (iterator.hasNextPoint()) {
            Binary value = Binary.valueOf(String.valueOf(count));
            assertEquals(count, iterator.nextTime());
            assertEquals(value, iterator.nextBinary());
            assertEquals(value.getStringValue(), iterator.nextString());
            iterator.next();
            assertEquals(count, iterator.currentTime());
            assertEquals(value, iterator.currentBinary());
            assertEquals(value.getStringValue(), iterator.currentString());
            ++count;
          }
          break;
      }
    } catch (IOException e) {
      fail(e.toString());
    }
    assertEquals(ITERATION_TIMES, count);
  }

  private void testOrderedAccessByTumblingTimeWindowIterator(TSDataType dataType,
      int displayWindowBegin) {
    int iterationTimes = ITERATION_TIMES - displayWindowBegin;
    int totalInFirstExecution = 0;
    int totalInSecondExecution = 0;
    try {
      // test different constructors
      DataPointWindowIterator windowIterator = iterationTimes == ITERATION_TIMES
          ? tvList.getTumblingTimeWindowIterator(BATCH_SIZE)
          : tvList.getTumblingTimeWindowIterator(BATCH_SIZE, displayWindowBegin);

      int windowCount = 0;
      while (windowIterator.hasNextWindow()) {
        windowIterator.next();
        testRandomAccessByIndexInDataPointWindow(dataType,
            displayWindowBegin + totalInFirstExecution, windowIterator);
        totalInFirstExecution = testDataPointIteratorGeneratedByDataPointWindowIterator(dataType,
            displayWindowBegin + windowIterator.currentWindowIndex() * BATCH_SIZE,
            windowIterator.currentWindow(), BATCH_SIZE, totalInFirstExecution);
        ++windowCount;
      }
      assertEquals(iterationTimes / BATCH_SIZE, windowCount);

      windowIterator.reset();

      windowCount = 0;
      while (windowIterator.hasNextWindow()) {
        windowIterator.next();
        testRandomAccessByIndexInDataPointWindow(dataType,
            displayWindowBegin + totalInSecondExecution, windowIterator);
        totalInSecondExecution = testDataPointIteratorGeneratedByDataPointWindowIterator(dataType,
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

  private void testOrderedAccessBySlidingTimeWindowIterator(TSDataType dataType,
      int displayWindowBegin, int displayWindowEnd, int timeInterval, int slidingStep) {
    int timeWindow = displayWindowEnd - displayWindowBegin;
    try {
      DataPointWindowIterator windowIterator = timeWindow == ITERATION_TIMES
          ? tvList.getSlidingTimeWindowIterator(timeInterval, slidingStep)
          : tvList.getSlidingTimeWindowIterator(timeInterval, slidingStep,
              displayWindowBegin, displayWindowEnd); // test different constructors

      testOrderedAccessBySlidingTimeWindowIteratorOnce(dataType, displayWindowBegin,
          displayWindowEnd, timeInterval, slidingStep, windowIterator);
      windowIterator.reset();
      testOrderedAccessBySlidingTimeWindowIteratorOnce(dataType, displayWindowBegin,
          displayWindowEnd, timeInterval, slidingStep, windowIterator);
    } catch (IOException | QueryProcessException e) {
      fail(e.toString());
    }
  }

  private void testOrderedAccessBySlidingTimeWindowIteratorOnce(TSDataType dataType,
      int displayWindowBegin, int displayWindowEnd, int timeInterval, int slidingStep,
      DataPointWindowIterator windowIterator) throws IOException {
    int timeWindow = displayWindowEnd - displayWindowBegin;
    int expectedTotal = 0;
    int actualTotal = 0;
    int windowCount = 0;

    while (windowIterator.hasNextWindow()) {
      windowIterator.next();
      int initialIndex = displayWindowBegin + windowIterator.currentWindowIndex() * slidingStep;
      testRandomAccessByIndexInDataPointWindow(dataType, initialIndex, windowIterator);
      int expectedWindowSize = displayWindowEnd < initialIndex + timeInterval
          ? displayWindowEnd - initialIndex : timeInterval;

      DataPointIterator dataPointIterator = windowIterator.currentWindow();
      int actualTotalInFirstExecution = testDataPointIteratorGeneratedByDataPointWindowIterator(
          dataType, initialIndex, dataPointIterator, expectedWindowSize, actualTotal);
      dataPointIterator.reset();
      actualTotal = testDataPointIteratorGeneratedByDataPointWindowIterator(dataType, initialIndex,
          dataPointIterator, expectedWindowSize, actualTotal);
      assertEquals(actualTotalInFirstExecution, actualTotal);

      expectedTotal += expectedWindowSize;
      ++windowCount;
    }

    assertEquals((int) Math.ceil((float) timeWindow / slidingStep), windowCount);
    assertEquals(expectedTotal, actualTotal);
  }

  private int testDataPointIteratorGeneratedByDataPointWindowIterator(TSDataType dataType,
      int initialIndex, DataPointIterator dataPointIterator, int expectedDataPointCount,
      int total) {
    int totalInFirstExecution = testDataPointIteratorGeneratedByDataPointWindowIteratorOnce(
        dataType,
        initialIndex, dataPointIterator, expectedDataPointCount, total);
    dataPointIterator.reset();
    total = testDataPointIteratorGeneratedByDataPointWindowIteratorOnce(dataType, initialIndex,
        dataPointIterator, expectedDataPointCount, total);
    assertEquals(totalInFirstExecution, total);
    return total;
  }

  private int testDataPointIteratorGeneratedByDataPointWindowIteratorOnce(TSDataType dataType,
      int initialIndex, DataPointIterator dataPointIterator, int expectedDataPointCount,
      int total) {
    int actualDataPointCount = 0;
    try {
      switch (dataType) {
        case INT32:
          while (dataPointIterator.hasNextPoint()) {
            int expected = initialIndex + actualDataPointCount;
            assertEquals(expected, dataPointIterator.nextTime());
            assertEquals(expected, dataPointIterator.nextInt());
            dataPointIterator.next();
            assertEquals(expected, dataPointIterator.currentTime());
            assertEquals(expected, dataPointIterator.currentInt());
            ++total;
            ++actualDataPointCount;
          }
          assertEquals(expectedDataPointCount, actualDataPointCount);
          break;
        case INT64:
          while (dataPointIterator.hasNextPoint()) {
            int expected = initialIndex + actualDataPointCount;
            assertEquals(expected, dataPointIterator.nextTime());
            assertEquals(expected, dataPointIterator.nextLong());
            dataPointIterator.next();
            assertEquals(expected, dataPointIterator.currentTime());
            assertEquals(expected, dataPointIterator.currentLong());
            ++total;
            ++actualDataPointCount;
          }
          assertEquals(String.valueOf(initialIndex), expectedDataPointCount, actualDataPointCount);
          break;
        case FLOAT:
          while (dataPointIterator.hasNextPoint()) {
            int expected = initialIndex + actualDataPointCount;
            assertEquals(expected, dataPointIterator.nextTime());
            assertEquals(expected, dataPointIterator.nextFloat(), 0);
            dataPointIterator.next();
            assertEquals(expected, dataPointIterator.currentTime());
            assertEquals(expected, dataPointIterator.currentFloat(), 0);
            ++total;
            ++actualDataPointCount;
          }
          assertEquals(expectedDataPointCount, actualDataPointCount);
          break;
        case DOUBLE:
          while (dataPointIterator.hasNextPoint()) {
            int expected = initialIndex + actualDataPointCount;
            assertEquals(expected, dataPointIterator.nextTime());
            assertEquals(expected, dataPointIterator.nextDouble(), 0);
            dataPointIterator.next();
            assertEquals(expected, dataPointIterator.currentTime());
            assertEquals(expected, dataPointIterator.currentDouble(), 0);
            ++total;
            ++actualDataPointCount;
          }
          assertEquals(expectedDataPointCount, actualDataPointCount);
          break;
        case BOOLEAN:
          while (dataPointIterator.hasNextPoint()) {
            int expected = initialIndex + actualDataPointCount;
            assertEquals(expected, dataPointIterator.nextTime());
            assertEquals(expected % 2 == 0, dataPointIterator.nextBoolean());
            dataPointIterator.next();
            assertEquals(expected, dataPointIterator.currentTime());
            assertEquals(expected % 2 == 0, dataPointIterator.currentBoolean());
            ++total;
            ++actualDataPointCount;
          }
          assertEquals(expectedDataPointCount, actualDataPointCount);
          break;
        case TEXT:
          while (dataPointIterator.hasNextPoint()) {
            int expected = initialIndex + actualDataPointCount;
            Binary value = Binary.valueOf(String.valueOf(expected));
            assertEquals(expected, dataPointIterator.nextTime());
            assertEquals(value, dataPointIterator.nextBinary());
            assertEquals(value.getStringValue(), dataPointIterator.nextString());
            dataPointIterator.next();
            assertEquals(expected, dataPointIterator.currentTime());
            assertEquals(value, dataPointIterator.currentBinary());
            assertEquals(value.getStringValue(), dataPointIterator.currentString());
            ++total;
            ++actualDataPointCount;
          }
          assertEquals(expectedDataPointCount, actualDataPointCount);
          break;
      }
    } catch (IOException e) {
      fail(e.toString());
    }
    return total;
  }

  private void testRandomAccessByIndexInDataPointWindow(TSDataType dataType, int initialIndex,
      DataPointWindowIterator windowIterator) {
    int windowSize = windowIterator.currentWindowSize();
    List<Integer> accessOrder = new ArrayList<>(windowSize);
    for (int i = 0; i < windowSize; ++i) {
      accessOrder.add(i);
    }
    Collections.shuffle(accessOrder);

    try {
      switch (dataType) {
        case INT32:
          for (int i = 0; i < windowSize; ++i) {
            int accessIndex = accessOrder.get(i);
            int expected = initialIndex + accessIndex;
            assertEquals(expected, windowIterator.getTimeInCurrentWindow(accessIndex));
            assertEquals(expected, windowIterator.getIntInCurrentWindow(accessIndex));
          }
          break;
        case INT64:
          for (int i = 0; i < windowSize; ++i) {
            int accessIndex = accessOrder.get(i);
            int expected = initialIndex + accessIndex;
            assertEquals(expected, windowIterator.getTimeInCurrentWindow(accessIndex));
            assertEquals(expected, windowIterator.getLongInCurrentWindow(accessIndex));
          }
          break;
        case FLOAT:
          for (int i = 0; i < windowSize; ++i) {
            int accessIndex = accessOrder.get(i);
            int expected = initialIndex + accessIndex;
            assertEquals(expected, windowIterator.getTimeInCurrentWindow(accessIndex));
            assertEquals(expected, windowIterator.getFloatInCurrentWindow(accessIndex), 0);
          }
          break;
        case DOUBLE:
          for (int i = 0; i < windowSize; ++i) {
            int accessIndex = accessOrder.get(i);
            int expected = initialIndex + accessIndex;
            assertEquals(expected, windowIterator.getTimeInCurrentWindow(accessIndex));
            assertEquals(expected, windowIterator.getDoubleInCurrentWindow(accessIndex), 0);
          }
          break;
        case BOOLEAN:
          for (int i = 0; i < windowSize; ++i) {
            int accessIndex = accessOrder.get(i);
            int expected = initialIndex + accessIndex;
            assertEquals(expected, windowIterator.getTimeInCurrentWindow(accessIndex));
            assertEquals(expected % 2 == 0, windowIterator.getBooleanInCurrentWindow(accessIndex));
          }
          break;
        case TEXT:
          for (int i = 0; i < windowSize; ++i) {
            int accessIndex = accessOrder.get(i);
            int expected = initialIndex + accessIndex;
            Binary value = Binary.valueOf(String.valueOf(expected));
            assertEquals(expected, windowIterator.getTimeInCurrentWindow(accessIndex));
            assertEquals(value, windowIterator.getBinaryInCurrentWindow(accessIndex));
            assertEquals(value.getStringValue(),
                windowIterator.getStringInCurrentWindow(accessIndex));
          }
          break;
      }
    } catch (IOException e) {
      fail(e.toString());
    }
  }
}
