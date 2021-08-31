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
package org.apache.iotdb.tsfile.file.metadata.statistics;

import org.apache.iotdb.tsfile.utils.Binary;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class StringStatisticsTest {

  @Test
  public void testUpdate() {
    Statistics<Binary> binaryStats = new BinaryStatistics();
    binaryStats.updateStats(new Binary("aaa"));
    assertFalse(binaryStats.isEmpty());
    binaryStats.updateStats(new Binary("bbb"));
    assertFalse(binaryStats.isEmpty());
    assertEquals("aaa", binaryStats.getFirstValue().getStringValue());
    assertEquals("bbb", binaryStats.getLastValue().getStringValue());
  }

  @Test
  public void testMerge() {
    Statistics<Binary> stringStats1 = new BinaryStatistics();
    stringStats1.setStartTime(0);
    stringStats1.setEndTime(2);
    Statistics<Binary> stringStats2 = new BinaryStatistics();
    stringStats2.setStartTime(3);
    stringStats2.setEndTime(5);

    stringStats1.updateStats(new Binary("aaa"));
    stringStats1.updateStats(new Binary("ccc"));

    stringStats2.updateStats(new Binary("ddd"));

    Statistics<Binary> stringStats3 = new BinaryStatistics();
    stringStats3.mergeStatistics(stringStats1);
    assertFalse(stringStats3.isEmpty());
    assertEquals("aaa", stringStats3.getFirstValue().getStringValue());
    assertEquals("ccc", stringStats3.getLastValue().getStringValue());

    stringStats3.mergeStatistics(stringStats2);
    assertEquals("aaa", stringStats3.getFirstValue().getStringValue());
    assertEquals("ddd", stringStats3.getLastValue().getStringValue());

    Statistics<Binary> stringStats4 = new BinaryStatistics();
    stringStats4.setStartTime(0);
    stringStats4.setEndTime(5);
    Statistics<Binary> stringStats5 = new BinaryStatistics();
    stringStats5.setStartTime(1);
    stringStats5.setEndTime(4);

    stringStats4.updateStats(new Binary("eee"));
    stringStats4.updateStats(new Binary("fff"));

    stringStats5.updateStats(new Binary("ggg"));

    stringStats3.mergeStatistics(stringStats4);
    assertEquals("eee", stringStats3.getFirstValue().getStringValue());
    assertEquals("fff", stringStats3.getLastValue().getStringValue());

    stringStats3.mergeStatistics(stringStats5);
    assertEquals("eee", stringStats3.getFirstValue().getStringValue());
    assertEquals("fff", stringStats3.getLastValue().getStringValue());
  }
}
