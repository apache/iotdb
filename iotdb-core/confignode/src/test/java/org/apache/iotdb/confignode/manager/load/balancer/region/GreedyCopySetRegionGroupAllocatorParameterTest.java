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

package org.apache.iotdb.confignode.manager.load.balancer.region;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

public class GreedyCopySetRegionGroupAllocatorParameterTest {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(GreedyCopySetRegionGroupAllocatorParameterTest.class);

  private static final int TEST_LOOP = 10;
  private static final int DATA_REPLICATION_FACTOR = 2;
  private static final int MIN_PLAN_NUM = 2;
  private static final int MAX_PLAN_NUM = 100;
  private static final int MIN_DATA_NODE_NUM = 2;
  private static final int MAX_DATA_NODE_NUM = 100;
  private static final int DATA_REGION_PER_DATA_NODE = 6;

  private static final Map<Integer, TDataNodeConfiguration> AVAILABLE_DATA_NODE_MAP =
      new TreeMap<>();
  private static final Map<Integer, Double> FREE_SPACE_MAP = new TreeMap<>();

  private static final class DataEntry {
    private final Integer minScatterWidth;
    private final Double avgTime;

    private DataEntry(Integer minScatterWidth, Double avgTime) {
      this.minScatterWidth = minScatterWidth;
      this.avgTime = avgTime;
    }
  }

  @Test
  public void loopTest() throws IOException {
    List<DataEntry> testResult = new ArrayList<>();
    for (int planNum = MIN_PLAN_NUM; planNum <= MAX_PLAN_NUM; planNum++) {
      ConfigNodeDescriptor.getInstance().getConf().setGcrMaxOptimalPlanNum(planNum);
      for (int dataNodeNum = MIN_DATA_NODE_NUM; dataNodeNum <= MAX_DATA_NODE_NUM; dataNodeNum++) {
        testResult.add(test(dataNodeNum));
      }
      LOGGER.info("{}, finish", planNum);
    }

    final String path = "/Users/yongzaodan/Desktop/simulation/gcr";
    FileWriter scatterW = new FileWriter(path + "/scatter.txt");
    FileWriter timeW = new FileWriter(path + "/avg.txt");
    for (DataEntry entry : testResult) {
      scatterW.write(entry.minScatterWidth.toString());
      scatterW.write("\n");
      scatterW.flush();
      timeW.write(entry.avgTime.toString());
      timeW.write("\n");
      timeW.flush();
    }
    scatterW.close();
    timeW.close();
  }

  private DataEntry test(int testDataNodeNum) {
    IRegionGroupAllocator ALLOCATOR = new GreedyCopySetRegionGroupAllocator();

    // Construct TEST_DATA_NODE_NUM DataNodes
    Random random = new Random();
    AVAILABLE_DATA_NODE_MAP.clear();
    FREE_SPACE_MAP.clear();
    for (int i = 1; i <= testDataNodeNum; i++) {
      AVAILABLE_DATA_NODE_MAP.put(
          i, new TDataNodeConfiguration().setLocation(new TDataNodeLocation().setDataNodeId(i)));
      FREE_SPACE_MAP.put(i, random.nextDouble());
    }

    long timeSum = 0;
    int minScatterWidth = Integer.MAX_VALUE;
    final int dataRegionGroupNum =
        DATA_REGION_PER_DATA_NODE * testDataNodeNum / DATA_REPLICATION_FACTOR;
    for (int loop = 0; loop < TEST_LOOP; loop++) {
      long startTime = System.nanoTime();
      /* Allocate RegionGroup */
      List<TRegionReplicaSet> allocateResult = new ArrayList<>();
      for (int index = 0; index < dataRegionGroupNum; index++) {
        allocateResult.add(
            ALLOCATOR.generateOptimalRegionReplicasDistribution(
                AVAILABLE_DATA_NODE_MAP,
                FREE_SPACE_MAP,
                allocateResult,
                allocateResult,
                DATA_REPLICATION_FACTOR,
                new TConsensusGroupId(TConsensusGroupType.DataRegion, index)));
      }
      timeSum += System.nanoTime() - startTime;

      /* Calculate scatter width for each DataNode */
      // Map<DataNodeId, ScatterWidth>
      Map<Integer, BitSet> scatterWidthMap = new TreeMap<>();
      for (TRegionReplicaSet replicaSet : allocateResult) {
        for (int i = 0; i < DATA_REPLICATION_FACTOR; i++) {
          for (int j = i + 1; j < DATA_REPLICATION_FACTOR; j++) {
            int dataNodeId1 = replicaSet.getDataNodeLocations().get(i).getDataNodeId();
            int dataNodeId2 = replicaSet.getDataNodeLocations().get(j).getDataNodeId();
            scatterWidthMap.computeIfAbsent(dataNodeId1, empty -> new BitSet()).set(dataNodeId2);
            scatterWidthMap.computeIfAbsent(dataNodeId2, empty -> new BitSet()).set(dataNodeId1);
          }
        }
      }
      for (int i = 1; i <= testDataNodeNum; i++) {
        minScatterWidth =
            Math.min(
                minScatterWidth,
                scatterWidthMap.containsKey(i) ? scatterWidthMap.get(i).cardinality() : 0);
      }
    }
    return new DataEntry(
        minScatterWidth, (double) timeSum / (double) (TEST_LOOP * dataRegionGroupNum) / 1000.0);
  }
}
