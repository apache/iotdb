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

package org.apache.iotdb.db.storageengine.dataregion.memtable;

import org.junit.Test;

import java.math.BigInteger;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MemChunkTimeRangeUtilsTest {

  @Test
  public void testSplitFakePageTimeRangesWithLongBoundaries() {
    List<long[]> pageTimeRanges =
        MemChunkTimeRangeUtils.splitFakePageTimeRanges(Long.MIN_VALUE, Long.MAX_VALUE, 100);

    assertEquals(100, pageTimeRanges.size());
    assertEquals(Long.MIN_VALUE, pageTimeRanges.get(0)[0]);
    assertEquals(Long.MAX_VALUE, pageTimeRanges.get(pageTimeRanges.size() - 1)[1]);
    for (int i = 0; i < pageTimeRanges.size(); i++) {
      long[] pageTimeRange = pageTimeRanges.get(i);
      assertTrue(pageTimeRange[0] <= pageTimeRange[1]);
      if (i > 0) {
        assertEquals(
            BigInteger.valueOf(pageTimeRanges.get(i - 1)[1]).add(BigInteger.ONE),
            BigInteger.valueOf(pageTimeRange[0]));
      }
    }
  }

  @Test
  public void testSplitFakePageTimeRangesDoesNotCreateEmptyRanges() {
    List<long[]> pageTimeRanges = MemChunkTimeRangeUtils.splitFakePageTimeRanges(10, 10, 100);

    assertEquals(1, pageTimeRanges.size());
    assertArrayEquals(new long[] {10, 10}, pageTimeRanges.get(0));
  }
}
