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

package org.apache.iotdb.db.queryengine.plan.planner.distribution;

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.db.queryengine.plan.planner.distribution.SourceRewriter.splitTimePartition;
import static org.junit.Assert.assertEquals;

public class SplitTimePartitionTest {

  @Test
  public void testSplitTimePartition1() {
    //                timepartition-1   timepartition-2   timepartition-3   timepartition-4
    // SeriesSlot-1   region-1          region-1          region-1          region-1
    // SeriesSlot-2   region-2          region-2          region-2          region-2
    // SeriesSlot-3   region-1          region-1          region-1          region-1
    // SeriesSlot-4   region-2          region-2          region-2          region-2
    List<List<List<TTimePartitionSlot>>> sourceTimeRangeList = new ArrayList<>();
    sourceTimeRangeList.add(new ArrayList<>());
    sourceTimeRangeList
        .get(0)
        .add(
            Arrays.asList(
                new TTimePartitionSlot(1),
                new TTimePartitionSlot(11),
                new TTimePartitionSlot(21),
                new TTimePartitionSlot(31)));
    sourceTimeRangeList.add(new ArrayList<>());
    sourceTimeRangeList
        .get(1)
        .add(
            Arrays.asList(
                new TTimePartitionSlot(1),
                new TTimePartitionSlot(11),
                new TTimePartitionSlot(21),
                new TTimePartitionSlot(31)));
    sourceTimeRangeList.add(new ArrayList<>());
    sourceTimeRangeList
        .get(2)
        .add(
            Arrays.asList(
                new TTimePartitionSlot(1),
                new TTimePartitionSlot(11),
                new TTimePartitionSlot(21),
                new TTimePartitionSlot(31)));
    sourceTimeRangeList.add(new ArrayList<>());
    sourceTimeRangeList
        .get(3)
        .add(
            Arrays.asList(
                new TTimePartitionSlot(1),
                new TTimePartitionSlot(11),
                new TTimePartitionSlot(21),
                new TTimePartitionSlot(31)));

    List<List<TTimePartitionSlot>> res = splitTimePartition(sourceTimeRangeList);
    assertEquals(1, res.size());
    assertEquals(4, res.get(0).size());
    assertEquals(1, res.get(0).get(0).startTime);
    assertEquals(11, res.get(0).get(1).startTime);
    assertEquals(21, res.get(0).get(2).startTime);
    assertEquals(31, res.get(0).get(3).startTime);
  }

  @Test
  public void testSplitTimePartition2() {
    /*
     *                timepartition-1   timepartition-2   timepartition-3   timepartition-4   timepartition-5
     * SeriesSlot-1   region-1          region-1          region-1          region-1          region-1
     * SeriesSlot-2   region-2          region-2          region-2          region-2          region-2
     * SeriesSlot-3   region-1          region-1          region-1          region-1          region-3
     * SeriesSlot-4   region-2          region-2          region-2          region-2          region-4
     */

    List<List<List<TTimePartitionSlot>>> sourceTimeRangeList = new ArrayList<>();
    sourceTimeRangeList.add(new ArrayList<>());
    sourceTimeRangeList
        .get(0)
        .add(
            Arrays.asList(
                new TTimePartitionSlot(1),
                new TTimePartitionSlot(11),
                new TTimePartitionSlot(21),
                new TTimePartitionSlot(31),
                new TTimePartitionSlot(41)));
    sourceTimeRangeList.add(new ArrayList<>());
    sourceTimeRangeList
        .get(1)
        .add(
            Arrays.asList(
                new TTimePartitionSlot(1),
                new TTimePartitionSlot(11),
                new TTimePartitionSlot(21),
                new TTimePartitionSlot(31),
                new TTimePartitionSlot(41)));
    sourceTimeRangeList.add(new ArrayList<>());
    sourceTimeRangeList
        .get(2)
        .add(
            Arrays.asList(
                new TTimePartitionSlot(1),
                new TTimePartitionSlot(11),
                new TTimePartitionSlot(21),
                new TTimePartitionSlot(31)));
    sourceTimeRangeList.get(2).add(Collections.singletonList(new TTimePartitionSlot(41)));
    sourceTimeRangeList.add(new ArrayList<>());
    sourceTimeRangeList
        .get(3)
        .add(
            Arrays.asList(
                new TTimePartitionSlot(1),
                new TTimePartitionSlot(11),
                new TTimePartitionSlot(21),
                new TTimePartitionSlot(31)));
    sourceTimeRangeList.get(3).add(Collections.singletonList(new TTimePartitionSlot(41)));

    List<List<TTimePartitionSlot>> res = splitTimePartition(sourceTimeRangeList);
    assertEquals(2, res.size());
    assertEquals(4, res.get(0).size());
    assertEquals(1, res.get(0).get(0).startTime);
    assertEquals(11, res.get(0).get(1).startTime);
    assertEquals(21, res.get(0).get(2).startTime);
    assertEquals(31, res.get(0).get(3).startTime);
    assertEquals(1, res.get(1).size());
    assertEquals(41, res.get(1).get(0).startTime);
  }

  @Test
  public void testSplitTimePartition3() {
    //                timepartition-1   timepartition-2   timepartition-3   timepartition-4
    // SeriesSlot-1   region-1          region-2          region-2          region-4
    // SeriesSlot-2   region-1          region-3          region-3          region-3
    // SeriesSlot-3   region-1          region-1          region-4          region-3
    // SeriesSlot-4   region-4          region-2          region-4          region-4
    List<List<List<TTimePartitionSlot>>> sourceTimeRangeList = new ArrayList<>();
    sourceTimeRangeList.add(new ArrayList<>());
    sourceTimeRangeList.get(0).add(Collections.singletonList(new TTimePartitionSlot(1)));
    sourceTimeRangeList
        .get(0)
        .add(Arrays.asList(new TTimePartitionSlot(11), new TTimePartitionSlot(21)));
    sourceTimeRangeList.get(0).add(Collections.singletonList(new TTimePartitionSlot(31)));

    sourceTimeRangeList.add(new ArrayList<>());
    sourceTimeRangeList.get(1).add(Collections.singletonList(new TTimePartitionSlot(1)));
    sourceTimeRangeList
        .get(1)
        .add(
            Arrays.asList(
                new TTimePartitionSlot(11),
                new TTimePartitionSlot(21),
                new TTimePartitionSlot(31)));
    sourceTimeRangeList.add(new ArrayList<>());
    sourceTimeRangeList
        .get(2)
        .add(Arrays.asList(new TTimePartitionSlot(1), new TTimePartitionSlot(11)));
    sourceTimeRangeList.get(2).add(Collections.singletonList(new TTimePartitionSlot(21)));
    sourceTimeRangeList.get(2).add(Collections.singletonList(new TTimePartitionSlot(31)));

    sourceTimeRangeList.add(new ArrayList<>());
    sourceTimeRangeList.get(3).add(Collections.singletonList(new TTimePartitionSlot(1)));
    sourceTimeRangeList.get(3).add(Collections.singletonList(new TTimePartitionSlot(11)));
    sourceTimeRangeList
        .get(3)
        .add(Arrays.asList(new TTimePartitionSlot(21), new TTimePartitionSlot(31)));

    List<List<TTimePartitionSlot>> res = splitTimePartition(sourceTimeRangeList);
    assertEquals(4, res.size());
    assertEquals(1, res.get(0).size());
    assertEquals(1, res.get(0).get(0).startTime);
    assertEquals(1, res.get(1).size());
    assertEquals(11, res.get(1).get(0).startTime);
    assertEquals(1, res.get(2).size());
    assertEquals(21, res.get(2).get(0).startTime);
    assertEquals(1, res.get(3).size());
    assertEquals(31, res.get(3).get(0).startTime);
  }

  @Test
  public void testSplitTimePartition4() {
    //                timepartition-1   timepartition-2   timepartition-3   timepartition-4
    // SeriesSlot-1                     region-2          region-2
    // SeriesSlot-2   region-1          region-1          region-1          region-1
    // SeriesSlot-3                                       region-3          region-3
    // SeriesSlot-4   region-4          region-4          region-4          region-4
    List<List<List<TTimePartitionSlot>>> sourceTimeRangeList = new ArrayList<>();
    sourceTimeRangeList.add(new ArrayList<>());
    sourceTimeRangeList
        .get(0)
        .add(Arrays.asList(new TTimePartitionSlot(11), new TTimePartitionSlot(21)));

    sourceTimeRangeList.add(new ArrayList<>());
    sourceTimeRangeList
        .get(1)
        .add(
            Arrays.asList(
                new TTimePartitionSlot(1),
                new TTimePartitionSlot(11),
                new TTimePartitionSlot(21),
                new TTimePartitionSlot(31)));

    sourceTimeRangeList.add(new ArrayList<>());
    sourceTimeRangeList
        .get(2)
        .add(Arrays.asList(new TTimePartitionSlot(21), new TTimePartitionSlot(31)));

    sourceTimeRangeList.add(new ArrayList<>());
    sourceTimeRangeList
        .get(3)
        .add(
            Arrays.asList(
                new TTimePartitionSlot(1),
                new TTimePartitionSlot(11),
                new TTimePartitionSlot(21),
                new TTimePartitionSlot(31)));

    List<List<TTimePartitionSlot>> res = splitTimePartition(sourceTimeRangeList);
    assertEquals(1, res.size());
    assertEquals(1, res.get(0).size());
    assertEquals(21, res.get(0).get(0).startTime);
  }

  @Test
  public void testSplitTimePartition5() {
    //                timepartition-1   timepartition-2   timepartition-3   timepartition-4
    // SeriesSlot-1                     region-2          region-2
    // SeriesSlot-2   region-1          region-1          region-2          region-2
    List<List<List<TTimePartitionSlot>>> sourceTimeRangeList = new ArrayList<>();
    sourceTimeRangeList.add(new ArrayList<>());
    sourceTimeRangeList
        .get(0)
        .add(Arrays.asList(new TTimePartitionSlot(11), new TTimePartitionSlot(21)));

    sourceTimeRangeList.add(new ArrayList<>());
    sourceTimeRangeList
        .get(1)
        .add(Arrays.asList(new TTimePartitionSlot(1), new TTimePartitionSlot(11)));
    sourceTimeRangeList
        .get(1)
        .add(Arrays.asList(new TTimePartitionSlot(21), new TTimePartitionSlot(31)));

    List<List<TTimePartitionSlot>> res = splitTimePartition(sourceTimeRangeList);
    assertEquals(2, res.size());
    assertEquals(1, res.get(0).size());
    assertEquals(11, res.get(0).get(0).startTime);
    assertEquals(1, res.get(1).size());
    assertEquals(21, res.get(1).get(0).startTime);
  }
}
