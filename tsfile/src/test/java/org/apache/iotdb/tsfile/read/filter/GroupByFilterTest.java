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
package org.apache.iotdb.tsfile.read.filter;

import org.apache.iotdb.tsfile.read.filter.factory.FilterType;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GroupByFilterTest {

  private GroupByFilter groupByFilter;

  @Before
  public void setUp() throws Exception {
    groupByFilter = new GroupByFilter(3, 24,
            8, 8 + 30 * 24 + 3 + 6, FilterType.GROUP_BY_FILTER);
  }

  @Test
  public void TestDigestSatisfy() {

    DigestForFilter digestForFilter = new DigestForFilter(0, 7,
            ByteBuffer.allocate(0), ByteBuffer.allocate(0), null);
    assertFalse(groupByFilter.satisfy(digestForFilter));

    digestForFilter = new DigestForFilter(8 + 30 * 24 + 3 + 6 + 1, 8 + 30 * 24 + 3 + 6 + 2,
            ByteBuffer.allocate(0), ByteBuffer.allocate(0), null);
    assertFalse(groupByFilter.satisfy(digestForFilter));

    digestForFilter = new DigestForFilter(0, 9,
            ByteBuffer.allocate(0), ByteBuffer.allocate(0), null);
    assertTrue(groupByFilter.satisfy(digestForFilter));

    digestForFilter = new DigestForFilter(32, 34,
            ByteBuffer.allocate(0), ByteBuffer.allocate(0), null);
    assertTrue(groupByFilter.satisfy(digestForFilter));

    digestForFilter = new DigestForFilter(32, 36,
            ByteBuffer.allocate(0), ByteBuffer.allocate(0), null);
    assertTrue(groupByFilter.satisfy(digestForFilter));

    digestForFilter = new DigestForFilter(36, 37,
            ByteBuffer.allocate(0), ByteBuffer.allocate(0), null);
    assertFalse(groupByFilter.satisfy(digestForFilter));

    digestForFilter = new DigestForFilter(36, 55,
            ByteBuffer.allocate(0), ByteBuffer.allocate(0), null);
    assertFalse(groupByFilter.satisfy(digestForFilter));

    digestForFilter = new DigestForFilter(35, 56,
            ByteBuffer.allocate(0), ByteBuffer.allocate(0), null);
    assertTrue(groupByFilter.satisfy(digestForFilter));

    digestForFilter = new DigestForFilter(35, 58,
            ByteBuffer.allocate(0), ByteBuffer.allocate(0), null);
    assertTrue(groupByFilter.satisfy(digestForFilter));

    digestForFilter = new DigestForFilter(8 + 30 * 24 + 3 + 1, 8 + 30 * 24 + 5,
            ByteBuffer.allocate(0), ByteBuffer.allocate(0), null);
    assertFalse(groupByFilter.satisfy(digestForFilter));

    digestForFilter = new DigestForFilter(8 + 30 * 24 + 3 + 1, 8 + 30 * 24 + 8,
            ByteBuffer.allocate(0), ByteBuffer.allocate(0), null);
    assertFalse(groupByFilter.satisfy(digestForFilter));
  }

  @Test
  public void TestSatisfy() {

    assertFalse(groupByFilter.satisfy(0, null));

    assertFalse(groupByFilter.satisfy(7, null));

    assertFalse(groupByFilter.satisfy(12, null));

    assertFalse(groupByFilter.satisfy(8 + 30 * 24 + 3 + 6, null));

    assertTrue(groupByFilter.satisfy(8, null));

    assertTrue(groupByFilter.satisfy(9, null));

    assertTrue(groupByFilter.satisfy(11, null));

  }


  @Test
  public void TestContainStartEndTime() {

    assertTrue(groupByFilter.containStartEndTime(8, 9));

    assertFalse(groupByFilter.containStartEndTime(8, 13));

    assertFalse(groupByFilter.containStartEndTime(0, 3));

    assertFalse(groupByFilter.containStartEndTime(0, 9));

    assertFalse(groupByFilter.containStartEndTime(7, 8 + 30 * 24 + 3 + 6 + 1));

    assertFalse(groupByFilter.containStartEndTime(8 + 30 * 24 + 3 + 6 + 1, 8 + 30 * 24 + 3 + 6 + 2));

  }

}
