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
import org.apache.iotdb.db.query.udf.api.iterator.DataPointBatchIterator;
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

    testOrderedAccessBySizeLimitedDataPointBatchIterator(dataType, 0);
    testOrderedAccessBySizeLimitedDataPointBatchIterator(dataType, ITERATION_TIMES / 2);

    testOrderedAccessByTimeWindowDataPointBatchIterator(dataType, 0, ITERATION_TIMES, BATCH_SIZE,
        BATCH_SIZE);
    testOrderedAccessByTimeWindowDataPointBatchIterator(dataType, (int) (0.25 * ITERATION_TIMES),
        (int) (0.75 * ITERATION_TIMES), BATCH_SIZE, BATCH_SIZE);

    testOrderedAccessByTimeWindowDataPointBatchIterator(dataType, 0, ITERATION_TIMES,
        BATCH_SIZE / 2, 2 * BATCH_SIZE);
    testOrderedAccessByTimeWindowDataPointBatchIterator(dataType, (int) (0.25 * ITERATION_TIMES),
        (int) (0.75 * ITERATION_TIMES), BATCH_SIZE / 2, 2 * BATCH_SIZE);

    testOrderedAccessByTimeWindowDataPointBatchIterator(dataType, 0, ITERATION_TIMES,
        2 * BATCH_SIZE, BATCH_SIZE / 2);
    testOrderedAccessByTimeWindowDataPointBatchIterator(dataType, (int) (0.25 * ITERATION_TIMES),
        (int) (0.75 * ITERATION_TIMES), 2 * BATCH_SIZE, BATCH_SIZE / 2);

    testOrderedAccessByTimeWindowDataPointBatchIterator(dataType, 0, 0, 2 * BATCH_SIZE,
        BATCH_SIZE / 2);

    testOrderedAccessByTimeWindowDataPointBatchIterator(dataType, (int) (0.25 * ITERATION_TIMES),
        (int) (0.6 * ITERATION_TIMES), 3 * BATCH_SIZE, (int) (0.3 * BATCH_SIZE));
    testOrderedAccessByTimeWindowDataPointBatchIterator(dataType, (int) (0.25 * ITERATION_TIMES),
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

  private void testOrderedAccessBySizeLimitedDataPointBatchIterator(TSDataType dataType,
      int displayWindowBegin) {
    int iterationTimes = ITERATION_TIMES - displayWindowBegin;
    int totalInFirstExecution = 0;
    int totalInSecondExecution = 0;
    try {
      // test different constructors
      DataPointBatchIterator batchIterator = iterationTimes == ITERATION_TIMES
          ? tvList.getSizeLimitedBatchIterator(BATCH_SIZE)
          : tvList.getSizeLimitedBatchIterator(BATCH_SIZE, displayWindowBegin);

      int batchCount = 0;
      while (batchIterator.hasNextBatch()) {
        batchIterator.next();
        testRandomAccessByIndexInDataPointBatch(dataType,
            displayWindowBegin + totalInFirstExecution, batchIterator);
        totalInFirstExecution = testDataPointIteratorGeneratedByDataPointBatchIterator(dataType,
            displayWindowBegin + batchIterator.currentBatchIndex() * BATCH_SIZE,
            batchIterator.currentBatch(), BATCH_SIZE, totalInFirstExecution);
        ++batchCount;
      }
      assertEquals(iterationTimes / BATCH_SIZE, batchCount);

      batchIterator.reset();

      batchCount = 0;
      while (batchIterator.hasNextBatch()) {
        batchIterator.next();
        testRandomAccessByIndexInDataPointBatch(dataType,
            displayWindowBegin + totalInSecondExecution, batchIterator);
        totalInSecondExecution = testDataPointIteratorGeneratedByDataPointBatchIterator(dataType,
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

  private void testOrderedAccessByTimeWindowDataPointBatchIterator(TSDataType dataType,
      int displayWindowBegin, int displayWindowEnd, int timeInterval, int slidingStep) {
    int timeWindow = displayWindowEnd - displayWindowBegin;
    try {
      DataPointBatchIterator batchIterator = timeWindow == ITERATION_TIMES
          ? tvList.getTimeWindowBatchIterator(timeInterval, slidingStep)
          : tvList.getTimeWindowBatchIterator(displayWindowBegin, displayWindowEnd,
              timeInterval, slidingStep); // test different constructors

      testOrderedAccessByTimeWindowDataPointBatchIteratorOnce(dataType, displayWindowBegin,
          displayWindowEnd, timeInterval, slidingStep, batchIterator);
      batchIterator.reset();
      testOrderedAccessByTimeWindowDataPointBatchIteratorOnce(dataType, displayWindowBegin,
          displayWindowEnd, timeInterval, slidingStep, batchIterator);
    } catch (IOException | QueryProcessException e) {
      fail(e.toString());
    }
  }

  private void testOrderedAccessByTimeWindowDataPointBatchIteratorOnce(TSDataType dataType,
      int displayWindowBegin, int displayWindowEnd, int timeInterval, int slidingStep,
      DataPointBatchIterator batchIterator) throws IOException {
    int timeWindow = displayWindowEnd - displayWindowBegin;
    int expectedTotal = 0;
    int actualTotal = 0;
    int batchCount = 0;

    while (batchIterator.hasNextBatch()) {
      batchIterator.next();
      int initialIndex = displayWindowBegin + batchIterator.currentBatchIndex() * slidingStep;
      testRandomAccessByIndexInDataPointBatch(dataType, initialIndex, batchIterator);
      int expectedBatchSize = displayWindowEnd < initialIndex + timeInterval
          ? displayWindowEnd - initialIndex : timeInterval;

      DataPointIterator dataPointIterator = batchIterator.currentBatch();
      int actualTotalInFirstExecution = testDataPointIteratorGeneratedByDataPointBatchIterator(
          dataType, initialIndex, dataPointIterator, expectedBatchSize, actualTotal);
      dataPointIterator.reset();
      actualTotal = testDataPointIteratorGeneratedByDataPointBatchIterator(dataType, initialIndex,
          dataPointIterator, expectedBatchSize, actualTotal);
      assertEquals(actualTotalInFirstExecution, actualTotal);

      expectedTotal += expectedBatchSize;
      ++batchCount;
    }

    assertEquals((int) Math.ceil((float) timeWindow / slidingStep), batchCount);
    assertEquals(expectedTotal, actualTotal);
  }

  private int testDataPointIteratorGeneratedByDataPointBatchIterator(TSDataType dataType,
      int initialIndex, DataPointIterator dataPointIterator, int expectedDataPointCount,
      int total) {
    int totalInFirstExecution = testDataPointIteratorGeneratedByDataPointBatchIteratorOnce(dataType,
        initialIndex, dataPointIterator, expectedDataPointCount, total);
    dataPointIterator.reset();
    total = testDataPointIteratorGeneratedByDataPointBatchIteratorOnce(dataType, initialIndex,
        dataPointIterator, expectedDataPointCount, total);
    assertEquals(totalInFirstExecution, total);
    return total;
  }

  private int testDataPointIteratorGeneratedByDataPointBatchIteratorOnce(TSDataType dataType,
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

  private void testRandomAccessByIndexInDataPointBatch(TSDataType dataType, int initialIndex,
      DataPointBatchIterator batchIterator) {
    int batchSize = batchIterator.currentBatchSize();
    List<Integer> accessOrder = new ArrayList<>(batchSize);
    for (int i = 0; i < batchSize; ++i) {
      accessOrder.add(i);
    }
    Collections.shuffle(accessOrder);

    try {
      switch (dataType) {
        case INT32:
          for (int i = 0; i < batchSize; ++i) {
            int accessIndex = accessOrder.get(i);
            int expected = initialIndex + accessIndex;
            assertEquals(expected, batchIterator.getTimeInCurrentBatch(accessIndex));
            assertEquals(expected, batchIterator.getIntInCurrentBatch(accessIndex));
          }
          break;
        case INT64:
          for (int i = 0; i < batchSize; ++i) {
            int accessIndex = accessOrder.get(i);
            int expected = initialIndex + accessIndex;
            assertEquals(expected, batchIterator.getTimeInCurrentBatch(accessIndex));
            assertEquals(expected, batchIterator.getLongInCurrentBatch(accessIndex));
          }
          break;
        case FLOAT:
          for (int i = 0; i < batchSize; ++i) {
            int accessIndex = accessOrder.get(i);
            int expected = initialIndex + accessIndex;
            assertEquals(expected, batchIterator.getTimeInCurrentBatch(accessIndex));
            assertEquals(expected, batchIterator.getFloatInCurrentBatch(accessIndex), 0);
          }
          break;
        case DOUBLE:
          for (int i = 0; i < batchSize; ++i) {
            int accessIndex = accessOrder.get(i);
            int expected = initialIndex + accessIndex;
            assertEquals(expected, batchIterator.getTimeInCurrentBatch(accessIndex));
            assertEquals(expected, batchIterator.getDoubleInCurrentBatch(accessIndex), 0);
          }
          break;
        case BOOLEAN:
          for (int i = 0; i < batchSize; ++i) {
            int accessIndex = accessOrder.get(i);
            int expected = initialIndex + accessIndex;
            assertEquals(expected, batchIterator.getTimeInCurrentBatch(accessIndex));
            assertEquals(expected % 2 == 0, batchIterator.getBooleanInCurrentBatch(accessIndex));
          }
          break;
        case TEXT:
          for (int i = 0; i < batchSize; ++i) {
            int accessIndex = accessOrder.get(i);
            int expected = initialIndex + accessIndex;
            Binary value = Binary.valueOf(String.valueOf(expected));
            assertEquals(expected, batchIterator.getTimeInCurrentBatch(accessIndex));
            assertEquals(value, batchIterator.getBinaryInCurrentBatch(accessIndex));
            assertEquals(value.getStringValue(),
                batchIterator.getStringInCurrentBatch(accessIndex));
          }
          break;
      }
    } catch (IOException e) {
      fail(e.toString());
    }
  }
}
