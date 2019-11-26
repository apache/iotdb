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

import java.nio.ByteBuffer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.filter.factory.FilterFactory;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.junit.Assert;
import org.junit.Test;

public class DigestFilterTest {

  private StatisticsForFilter statistics1 = new StatisticsForFilter(1L, 100L, BytesUtils.intToBytes(1),
      BytesUtils.intToBytes(100), TSDataType.INT32);
  private StatisticsForFilter statistics2 = new StatisticsForFilter(101L, 200L, BytesUtils.intToBytes(101),
      BytesUtils.intToBytes(200), TSDataType.INT32);
  private StatisticsForFilter statistics3 = new StatisticsForFilter(101L, 200L, (ByteBuffer) null, null,
      TSDataType.INT32);

  @Test
  public void testEq() {
    Filter timeEq = TimeFilter.eq(10L);
    Assert.assertTrue(timeEq.satisfy(statistics1));
    Assert.assertFalse(timeEq.satisfy(statistics2));
    Assert.assertFalse(timeEq.satisfy(statistics3));

    Filter valueEq = ValueFilter.eq(100);
    Assert.assertTrue(valueEq.satisfy(statistics1));
    Assert.assertFalse(valueEq.satisfy(statistics2));
    Assert.assertTrue(valueEq.satisfy(statistics3));
  }

  @Test
  public void testGt() {
    Filter timeGt = TimeFilter.gt(100L);
    Assert.assertFalse(timeGt.satisfy(statistics1));
    Assert.assertTrue(timeGt.satisfy(statistics2));
    Assert.assertTrue(timeGt.satisfy(statistics3));

    Filter valueGt = ValueFilter.gt(100);
    Assert.assertFalse(valueGt.satisfy(statistics1));
    Assert.assertTrue(valueGt.satisfy(statistics2));
    Assert.assertTrue(valueGt.satisfy(statistics3));
  }

  @Test
  public void testGtEq() {
    Filter timeGtEq = TimeFilter.gtEq(100L);
    Assert.assertTrue(timeGtEq.satisfy(statistics1));
    Assert.assertTrue(timeGtEq.satisfy(statistics2));
    Assert.assertTrue(timeGtEq.satisfy(statistics3));

    Filter valueGtEq = ValueFilter.gtEq(100);
    Assert.assertTrue(valueGtEq.satisfy(statistics1));
    Assert.assertTrue(valueGtEq.satisfy(statistics3));
    Assert.assertTrue(valueGtEq.satisfy(statistics3));
  }

  @Test
  public void testLt() {
    Filter timeLt = TimeFilter.lt(101L);
    Assert.assertTrue(timeLt.satisfy(statistics1));
    Assert.assertFalse(timeLt.satisfy(statistics2));
    Assert.assertFalse(timeLt.satisfy(statistics3));

    Filter valueLt = ValueFilter.lt(101);
    Assert.assertTrue(valueLt.satisfy(statistics1));
    Assert.assertFalse(valueLt.satisfy(statistics2));
    Assert.assertTrue(valueLt.satisfy(statistics3));
  }

  @Test
  public void testLtEq() {
    Filter timeLtEq = TimeFilter.ltEq(101L);
    Assert.assertTrue(timeLtEq.satisfy(statistics1));
    Assert.assertTrue(timeLtEq.satisfy(statistics2));
    Assert.assertTrue(timeLtEq.satisfy(statistics3));

    Filter valueLtEq = ValueFilter.ltEq(101);
    Assert.assertTrue(valueLtEq.satisfy(statistics1));
    Assert.assertTrue(valueLtEq.satisfy(statistics2));
    Assert.assertTrue(valueLtEq.satisfy(statistics3));
  }

  @Test
  public void testAndOr() {
    Filter andFilter = FilterFactory.and(TimeFilter.gt(10L), ValueFilter.lt(50));
    Assert.assertTrue(andFilter.satisfy(statistics1));
    Assert.assertFalse(andFilter.satisfy(statistics2));
    Assert.assertTrue(andFilter.satisfy(statistics3));

    Filter orFilter = FilterFactory.or(andFilter, TimeFilter.eq(200L));
    Assert.assertTrue(orFilter.satisfy(statistics1));
    Assert.assertTrue(orFilter.satisfy(statistics2));
    Assert.assertTrue(orFilter.satisfy(statistics3));
  }

}
