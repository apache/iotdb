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
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class PriorityMergeReaderTest {

  @Test
  public void test1() throws IOException {
    // 2 series
    testAscAndDescPriorityMergeReader(
        new long[] {1, 2, 3, 4, 5, 6},
        new long[] {2, 2, 2, 1, 2, 2},
        new long[] {1, 2, 3, 4, 5},
        new long[] {1, 2, 3, 5, 6});
    testAscAndDescPriorityMergeReader(
        new long[] {1, 2, 3, 4, 5},
        new long[] {1, 1, 1, 1, 1},
        new long[] {1, 2, 3, 4, 5},
        new long[] {});
    testAscAndDescPriorityMergeReader(
        new long[] {1, 2, 3, 4, 5},
        new long[] {2, 2, 2, 2, 2},
        new long[] {},
        new long[] {1, 2, 3, 4, 5});
    testAscAndDescPriorityMergeReader(
        new long[] {1, 2, 3, 4, 5, 6, 7, 8},
        new long[] {1, 1, 1, 1, 1, 2, 2, 2},
        new long[] {1, 2, 3, 4, 5},
        new long[] {6, 7, 8});

    // 3 series
    testAscAndDescPriorityMergeReader(
        new long[] {1, 2, 3, 4, 5, 6, 7},
        new long[] {3, 3, 3, 1, 3, 2, 3},
        new long[] {1, 2, 3, 4, 5},
        new long[] {1, 2, 3, 5, 6},
        new long[] {1, 2, 3, 5, 7});
    testAscAndDescPriorityMergeReader(
        new long[] {1, 2, 3, 4, 5, 6},
        new long[] {1, 1, 2, 3, 2, 3},
        new long[] {1, 2},
        new long[] {3, 5},
        new long[] {4, 6});
  }

  private void testAscAndDescPriorityMergeReader(
      long[] retTimestamp, long[] retValue, long[]... sourceTimestamps) throws IOException {
    testAsc(retTimestamp, retValue, sourceTimestamps);
    testDesc(retTimestamp, retValue, sourceTimestamps);
  }

  private void testAsc(long[] retTimestamp, long[] retValue, long[]... sourceTimestamps)
      throws IOException {
    PriorityMergeReader priorityMergeReader = new PriorityMergeReader();
    long mockUsedMemory = 0;
    for (int i = 0; i < sourceTimestamps.length; i++) {
      // priorityMergeReader.addReader(new FakedSeriesReader(sourceTimestamps[i], i + 1), i + 1);
      long endTime =
          sourceTimestamps[i].length == 0
              ? Long.MIN_VALUE
              : sourceTimestamps[i][sourceTimestamps[i].length - 1];
      priorityMergeReader.addReader(
          new AscFakedSeriesReader(sourceTimestamps[i], i + 1),
          new MergeReaderPriority(Long.MAX_VALUE, i + 1, 0, false),
          endTime);
      mockUsedMemory += sourceTimestamps[i].length;
    }

    // use the size of all timestamps to mock the used memory
    assertEquals(mockUsedMemory, priorityMergeReader.getUsedMemorySize());

    int i = 0;
    while (priorityMergeReader.hasNextTimeValuePair()) {
      TimeValuePair timeValuePair = priorityMergeReader.nextTimeValuePair();
      assertEquals(retTimestamp[i], timeValuePair.getTimestamp());
      assertEquals(retValue[i], timeValuePair.getValue().getValue());
      i++;
    }
  }

  private void testDesc(long[] retTimestamp, long[] retValue, long[]... sourceTimestamps)
      throws IOException {
    PriorityMergeReader descPriorityMergeReader = new DescPriorityMergeReader();
    long mockUsedMemory = 0;
    for (int i = 0; i < sourceTimestamps.length; i++) {
      // priorityMergeReader.addReader(new FakedSeriesReader(sourceTimestamps[i], i + 1), i + 1);
      long endTime =
          sourceTimestamps[i].length == 0
              ? Long.MIN_VALUE
              : sourceTimestamps[i][sourceTimestamps[i].length - 1];
      descPriorityMergeReader.addReader(
          new DescFakedSeriesReader(sourceTimestamps[i], i + 1),
          new MergeReaderPriority(Long.MAX_VALUE, i + 1, 0, false),
          endTime);
      mockUsedMemory += sourceTimestamps[i].length;
    }

    // use the size of all timestamps to mock the used memory
    assertEquals(mockUsedMemory, descPriorityMergeReader.getUsedMemorySize());

    int i = retTimestamp.length - 1;
    while (descPriorityMergeReader.hasNextTimeValuePair()) {
      TimeValuePair timeValuePair = descPriorityMergeReader.nextTimeValuePair();
      assertEquals(retTimestamp[i], timeValuePair.getTimestamp());
      assertEquals(retValue[i], timeValuePair.getValue().getValue());
      i--;
    }
  }

  @Test
  public void testAscPriorityMergeReader2() throws IOException {
    AscFakedSeriesReader reader1 = new AscFakedSeriesReader(100, 80, 5, 11);
    AscFakedSeriesReader reader2 = new AscFakedSeriesReader(150, 60, 6, 19);
    AscFakedSeriesReader reader3 = new AscFakedSeriesReader(180, 50, 7, 31);

    PriorityMergeReader priorityMergeReader = new PriorityMergeReader();
    priorityMergeReader.addReader(
        reader1, new MergeReaderPriority(Long.MAX_VALUE, 3, 0, false), 500);
    priorityMergeReader.addReader(
        reader2, new MergeReaderPriority(Long.MAX_VALUE, 2, 0, false), 510);
    priorityMergeReader.addReader(
        reader3, new MergeReaderPriority(Long.MAX_VALUE, 1, 0, false), 530);

    // use the size of all timestamps to mock the used memory
    assertEquals(80 + 60 + 50, priorityMergeReader.getUsedMemorySize());

    int cnt = 0;
    while (priorityMergeReader.hasNextTimeValuePair()) {
      TimeValuePair timeValuePair = priorityMergeReader.nextTimeValuePair();
      long time = timeValuePair.getTimestamp();
      long value = (long) timeValuePair.getValue().getValue();

      // System.out.println(time + "," + value);
      if (time <= 500 && (time - 100) % 5 == 0) {
        assertEquals(time % 11, value);
      } else if (time <= 510 && (time - 150) % 6 == 0) {
        assertEquals(time % 19, value);
      } else {
        assertEquals(time % 31, value);
      }
      cnt++;
    }
    assertEquals(162, cnt);
  }
}
