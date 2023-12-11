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

import org.apache.iotdb.tsfile.file.metadata.IMetadata;
import org.apache.iotdb.tsfile.file.metadata.statistics.LongStatistics;
import org.apache.iotdb.tsfile.read.filter.factory.TimeFilterApi;
import org.apache.iotdb.tsfile.read.filter.operator.GroupByFilter;

import org.junit.Before;
import org.junit.Test;

import static org.apache.iotdb.tsfile.read.filter.FilterTestUtil.newMetadata;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GroupByFilterTest {

  private GroupByFilter groupByFilter;

  @Before
  public void setUp() {
    groupByFilter = TimeFilterApi.groupBy(8, 8 + 30 * 24 + 3 + 6, 3, 24);
  }

  @Test
  public void TestStatisticsSatisfy() {

    LongStatistics statistics = new LongStatistics();
    statistics.setStartTime(0);
    statistics.setEndTime(7);
    IMetadata metadata = newMetadata(statistics);

    assertTrue(groupByFilter.canSkip(metadata));

    statistics.setStartTime(8 + 30 * 24 + 3 + 6 + 1);
    statistics.setEndTime(8 + 30 * 24 + 3 + 6 + 2);
    assertTrue(groupByFilter.canSkip(metadata));

    statistics.setStartTime(0);
    statistics.setEndTime(9);
    assertFalse(groupByFilter.canSkip(metadata));

    statistics.setStartTime(32);
    statistics.setEndTime(34);
    assertFalse(groupByFilter.canSkip(metadata));

    statistics.setStartTime(32);
    statistics.setEndTime(36);
    assertFalse(groupByFilter.canSkip(metadata));

    statistics.setStartTime(36);
    statistics.setEndTime(37);
    assertTrue(groupByFilter.canSkip(metadata));

    statistics.setStartTime(36);
    statistics.setEndTime(55);
    assertTrue(groupByFilter.canSkip(metadata));

    statistics.setStartTime(35);
    statistics.setEndTime(56);
    assertFalse(groupByFilter.canSkip(metadata));

    statistics.setStartTime(35);
    statistics.setEndTime(58);
    assertFalse(groupByFilter.canSkip(metadata));

    statistics.setStartTime(8 + 30 * 24 + 3 + 1);
    statistics.setEndTime(8 + 30 * 24 + 5);
    assertTrue(groupByFilter.canSkip(metadata));

    statistics.setStartTime(8 + 30 * 24 + 3 + 1);
    statistics.setEndTime(8 + 30 * 24 + 8);
    assertTrue(groupByFilter.canSkip(metadata));
  }

  @Test
  public void TestSatisfy() {

    assertFalse(groupByFilter.satisfy(0, null));

    assertFalse(groupByFilter.satisfy(7, null));

    assertFalse(groupByFilter.satisfy(12, null));

    assertFalse(groupByFilter.satisfy(8 + 30 * 24 + 3 + 6, null));

    assertTrue(groupByFilter.satisfy(8, null));

    assertTrue(groupByFilter.satisfy(9, null));

    assertFalse(groupByFilter.satisfy(11, null));
  }

  @Test
  public void TestContainStartEndTime() {

    assertTrue(groupByFilter.containStartEndTime(8, 9));

    assertFalse(groupByFilter.containStartEndTime(8, 13));

    assertFalse(groupByFilter.containStartEndTime(0, 3));

    assertFalse(groupByFilter.containStartEndTime(0, 9));

    assertFalse(groupByFilter.containStartEndTime(7, 8 + 30 * 24 + 3 + 6 + 1));

    assertFalse(
        groupByFilter.containStartEndTime(8 + 30 * 24 + 3 + 6 + 1, 8 + 30 * 24 + 3 + 6 + 2));
  }
}
