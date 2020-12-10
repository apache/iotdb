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
package org.apache.iotdb.db.query.dataset.groupby;

import java.io.IOException;
import org.apache.iotdb.db.qp.physical.crud.GroupByTimePlan;
import org.apache.iotdb.tsfile.utils.Pair;
import org.junit.Assert;
import org.junit.Test;

public class GroupByEngineDataSetTest {

  /**
   * Sliding step > unit && last time interval = unit
   */
  @Test
  public void calNextTimePartitionTest1() throws IOException {
    long queryId = 1000L;
    long unit = 3;
    long slidingStep = 5;
    long startTime = 8;
    long endTime = 8 + 4 * 5 + 3;

    long[] startTimeArray = {8, 13, 18, 23, 28};
    long[] endTimeArray = {11, 16, 21, 26, 31};

    GroupByTimePlan groupByTimePlan = new GroupByTimePlan();
    groupByTimePlan.setInterval(unit);
    groupByTimePlan.setSlidingStep(slidingStep);
    groupByTimePlan.setStartTime(startTime);
    groupByTimePlan.setEndTime(endTime);

    GroupByEngineDataSet groupByEngine = new GroupByWithValueFilterDataSet(queryId, groupByTimePlan);
    int cnt = 0;
    while (groupByEngine.hasNext()) {
      Pair pair = groupByEngine.nextTimePartition();
      Assert.assertEquals(startTimeArray[cnt], pair.left);
      Assert.assertEquals(endTimeArray[cnt], pair.right);
      cnt++;
    }
    Assert.assertEquals(startTimeArray.length, cnt);
  }

  /**
   * Sliding step = unit && last time interval = unit
   */
  @Test
  public void calNextTimePartitionTest2() throws IOException {
    long queryId = 1000L;
    long unit = 3;
    long slidingStep = 3;
    long startTime = 8;
    long endTime = 8 + 5 * 3;

    long[] startTimeArray = {8, 11, 14, 17, 20};
    long[] endTimeArray = {11, 14, 17, 20, 23};

    GroupByTimePlan groupByTimePlan = new GroupByTimePlan();
    groupByTimePlan.setInterval(unit);
    groupByTimePlan.setSlidingStep(slidingStep);
    groupByTimePlan.setStartTime(startTime);
    groupByTimePlan.setEndTime(endTime);
    GroupByEngineDataSet groupByEngine = new GroupByWithValueFilterDataSet(queryId, groupByTimePlan);
    int cnt = 0;
    while (groupByEngine.hasNext()) {
      Pair pair = groupByEngine.nextTimePartition();
      Assert.assertEquals(startTimeArray[cnt], pair.left);
      Assert.assertEquals(endTimeArray[cnt], pair.right);
      cnt++;
    }
    Assert.assertEquals(startTimeArray.length, cnt);
  }

  /**
   * Sliding step = unit && last time interval < unit
   */
  @Test
  public void calNextTimePartitionTest3() throws IOException {
    long queryId = 1000L;
    long unit = 3;
    long slidingStep = 3;
    long startTime = 8;
    long endTime = 8 + 5 * 3 + 2;

    long[] startTimeArray = {8, 11, 14, 17, 20, 23};
    long[] endTimeArray = {11, 14, 17, 20, 23, 25};

    GroupByTimePlan groupByTimePlan = new GroupByTimePlan();
    groupByTimePlan.setInterval(unit);
    groupByTimePlan.setSlidingStep(slidingStep);
    groupByTimePlan.setStartTime(startTime);
    groupByTimePlan.setEndTime(endTime);

    GroupByEngineDataSet groupByEngine = new GroupByWithValueFilterDataSet(queryId, groupByTimePlan);
    int cnt = 0;
    while (groupByEngine.hasNext()) {
      Pair pair = groupByEngine.nextTimePartition();
      Assert.assertEquals(startTimeArray[cnt], pair.left);
      Assert.assertEquals(endTimeArray[cnt], pair.right);
      cnt++;
    }
    Assert.assertEquals(startTimeArray.length, cnt);
  }

  /**
   * Desc query && sliding step > unit && last time interval = unit
   */
  @Test
  public void calNextTimePartitionDescTest1() throws IOException {
    long queryId = 1000L;
    long unit = 3;
    long slidingStep = 5;
    long startTime = 8;
    long endTime = 8 + 4 * 5 + 3;

    long[] startTimeArray = {28, 23, 18, 13, 8};
    long[] endTimeArray = {31, 26, 21, 16, 11};

    GroupByTimePlan groupByTimePlan = new GroupByTimePlan();
    groupByTimePlan.setAscending(false);
    groupByTimePlan.setInterval(unit);
    groupByTimePlan.setSlidingStep(slidingStep);
    groupByTimePlan.setStartTime(startTime);
    groupByTimePlan.setEndTime(endTime);

    GroupByEngineDataSet groupByEngine = new GroupByWithValueFilterDataSet(queryId, groupByTimePlan);
    int cnt = 0;
    while (groupByEngine.hasNext()) {
      Pair pair = groupByEngine.nextTimePartition();
      Assert.assertEquals(startTimeArray[cnt], pair.left);
      Assert.assertEquals(endTimeArray[cnt], pair.right);
      cnt++;
    }
    Assert.assertEquals(startTimeArray.length, cnt);
  }

  /**
   * Desc query && Sliding step = unit && last time interval = unit
   */
  @Test
  public void calNextTimePartitionDescTest2() throws IOException {
    long queryId = 1000L;
    long unit = 3;
    long slidingStep = 3;
    long startTime = 8;
    long endTime = 8 + 5 * 3;

    long[] startTimeArray = {20, 17, 14, 11, 8};
    long[] endTimeArray = {23, 20, 17, 14, 11};

    GroupByTimePlan groupByTimePlan = new GroupByTimePlan();
    groupByTimePlan.setAscending(false);
    groupByTimePlan.setInterval(unit);
    groupByTimePlan.setSlidingStep(slidingStep);
    groupByTimePlan.setStartTime(startTime);
    groupByTimePlan.setEndTime(endTime);
    GroupByEngineDataSet groupByEngine = new GroupByWithValueFilterDataSet(queryId, groupByTimePlan);
    int cnt = 0;
    while (groupByEngine.hasNext()) {
      Pair pair = groupByEngine.nextTimePartition();
      Assert.assertEquals(startTimeArray[cnt], pair.left);
      Assert.assertEquals(endTimeArray[cnt], pair.right);
      cnt++;
    }
    Assert.assertEquals(startTimeArray.length, cnt);
  }

  /**
   * Desc query && Sliding step = unit && last time interval < unit
   */
  @Test
  public void calNextTimePartitionDescTest3() throws IOException {
    long queryId = 1000L;
    long unit = 3;
    long slidingStep = 3;
    long startTime = 8;
    long endTime = 8 + 5 * 3 + 2;

    long[] startTimeArray = {23, 20, 17, 14, 11, 8};
    long[] endTimeArray = {25, 23, 20, 17, 14, 11};

    GroupByTimePlan groupByTimePlan = new GroupByTimePlan();
    groupByTimePlan.setAscending(false);
    groupByTimePlan.setInterval(unit);
    groupByTimePlan.setSlidingStep(slidingStep);
    groupByTimePlan.setStartTime(startTime);
    groupByTimePlan.setEndTime(endTime);

    GroupByEngineDataSet groupByEngine = new GroupByWithValueFilterDataSet(queryId, groupByTimePlan);
    int cnt = 0;
    while (groupByEngine.hasNext()) {
      Pair pair = groupByEngine.nextTimePartition();
      Assert.assertTrue(cnt < startTimeArray.length);
      Assert.assertEquals(startTimeArray[cnt], pair.left);
      Assert.assertEquals(endTimeArray[cnt], pair.right);
      cnt++;
    }
    Assert.assertEquals(startTimeArray.length, cnt);
  }
}