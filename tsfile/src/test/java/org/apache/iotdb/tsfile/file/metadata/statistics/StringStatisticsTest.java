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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.junit.Test;

import org.apache.iotdb.tsfile.file.metadata.statistics.BinaryStatistics;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.utils.Binary;

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
    Statistics<Binary> stringStats2 = new BinaryStatistics();

    stringStats1.updateStats(new Binary("aaa"));
    stringStats1.updateStats(new Binary("ccc"));

    stringStats2.updateStats(new Binary("ddd"));

    Statistics<Binary> stringStats3 = new BinaryStatistics();
    stringStats3.mergeStatistics(stringStats1);
    assertFalse(stringStats3.isEmpty());
    assertEquals("aaa", stringStats3.getFirstValue().getStringValue());
    assertEquals("ccc", stringStats3.getLastValue().getStringValue());

    stringStats3.mergeStatistics(stringStats2);
    assertEquals("aaa", (String) stringStats3.getFirstValue().getStringValue());
    assertEquals("ddd", stringStats3.getLastValue().getStringValue());
  }
}
