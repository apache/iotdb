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
package org.apache.iotdb.db.query.executor;

import org.apache.iotdb.db.query.dataset.groupby.GroupByEngineDataSet;
import org.apache.iotdb.db.query.dataset.groupby.GroupByWithValueFilterDataSet;
import org.apache.iotdb.tsfile.utils.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GroupByEngineDataSetTest {

  @Test
  public void test1() throws IOException {
    long jobId = 1000L;
    long unit = 20;
    long startTimePoint = 810;
    List<Pair<Long, Long>> pairList = new ArrayList<>();
    pairList.add(new Pair<>(805L, 811L));
    pairList.add(new Pair<>(825L, 849L));

    long[] startTimeArray = {805, 810, 830};
    long[] endTimeArray = {810, 830, 850};
    GroupByEngineDataSet groupByEngine = new GroupByWithValueFilterDataSet(jobId, null, unit,
        startTimePoint, pairList);
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

  @Test
  public void test2() throws IOException {
    long jobId = 1000L;
    long unit = 20;
    long startTimePoint = 850;
    List<Pair<Long, Long>> pairList = new ArrayList<>();
    pairList.add(new Pair<>(805L, 835L));
    pairList.add(new Pair<>(850L, 855L));
    pairList.add(new Pair<>(858L, 860L));
    pairList.add(new Pair<>(1200L, 1220L));

    long[] startTimeArray = {805, 810, 830, 850, 1200, 1210};
    long[] endTimeArray = {810, 830, 850, 870, 1210, 1230};
    GroupByEngineDataSet groupByEngine = new GroupByWithValueFilterDataSet(jobId, null, unit,
        startTimePoint, pairList);
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

  @Test
  public void test3() throws IOException {
    long jobId = 1000L;
    long unit = 20;
    long startTimePoint = 100;
    List<Pair<Long, Long>> pairList = new ArrayList<>();
    pairList.add(new Pair<>(805L, 835L));
    pairList.add(new Pair<>(850L, 855L));
    pairList.add(new Pair<>(858L, 860L));
    pairList.add(new Pair<>(1200L, 1220L));

    long[] startTimeArray = {805, 820, 850, 860, 1200, 1220};
    long[] endTimeArray = {820, 840, 860, 880, 1220, 1240};
    GroupByEngineDataSet groupByEngine = new GroupByWithValueFilterDataSet(jobId, null, unit,
        startTimePoint, pairList);
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

  @Test
  public void test4() throws IOException {
    long jobId = 1000L;
    long unit = 200;
    long startTimePoint = 100;
    List<Pair<Long, Long>> pairList = new ArrayList<>();
    pairList.add(new Pair<>(805L, 835L));
    pairList.add(new Pair<>(850L, 855L));
    pairList.add(new Pair<>(858L, 860L));
    pairList.add(new Pair<>(1200L, 1220L));

    long[] startTimeArray = {805, 1200};
    long[] endTimeArray = {900, 1300};
    GroupByEngineDataSet groupByEngine = new GroupByWithValueFilterDataSet(jobId, null, unit,
        startTimePoint, pairList);
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

  //(80ms, 30,[50,100], [585,590], [615, 650])
  @Test
  public void test5() throws IOException {
    long jobId = 1000L;
    long unit = 80;
    long startTimePoint = 30;
    List<Pair<Long, Long>> pairList = new ArrayList<>();
    pairList.add(new Pair<>(50L, 100L));
    pairList.add(new Pair<>(585L, 590L));
    pairList.add(new Pair<>(615L, 650L));

    long[] startTimeArray = {50, 585, 590};
    long[] endTimeArray = {110, 590, 670};
    GroupByEngineDataSet groupByEngine = new GroupByWithValueFilterDataSet(jobId, null, unit,
        startTimePoint, pairList);
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