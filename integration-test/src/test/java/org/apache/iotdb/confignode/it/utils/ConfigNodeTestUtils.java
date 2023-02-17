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
package org.apache.iotdb.confignode.it.utils;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSeriesPartitionSlot;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.confignode.rpc.thrift.IConfigNodeRPCService;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
import org.apache.iotdb.confignode.rpc.thrift.TTimeSlotList;
import org.apache.iotdb.it.env.ConfigNodeWrapper;
import org.apache.iotdb.it.env.DataNodeWrapper;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import org.apache.thrift.TException;
import org.junit.Assert;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ConfigNodeTestUtils {
  private static final int retryNum = 30;

  public static TShowClusterResp getClusterNodeInfos(
      IConfigNodeRPCService.Iface client, int expectedConfigNodeNum, int expectedDataNodeNum)
      throws TException, InterruptedException {
    TShowClusterResp clusterNodes = null;
    for (int i = 0; i < retryNum; i++) {
      clusterNodes = client.showCluster();
      if (clusterNodes.getConfigNodeListSize() == expectedConfigNodeNum
          && clusterNodes.getDataNodeListSize() == expectedDataNodeNum) {
        break;
      }
      Thread.sleep(1000);
    }

    assertEquals(expectedConfigNodeNum, clusterNodes.getConfigNodeListSize());
    assertEquals(expectedDataNodeNum, clusterNodes.getDataNodeListSize());

    return clusterNodes;
  }

  public static void checkNodeConfig(
      List<TConfigNodeLocation> configNodeList,
      List<TDataNodeLocation> dataNodeList,
      List<ConfigNodeWrapper> configNodeWrappers,
      List<DataNodeWrapper> dataNodeWrappers) {
    // check ConfigNode
    for (TConfigNodeLocation configNodeLocation : configNodeList) {
      boolean found = false;
      for (ConfigNodeWrapper configNodeWrapper : configNodeWrappers) {
        if (configNodeWrapper.getIp().equals(configNodeLocation.getInternalEndPoint().getIp())
            && configNodeWrapper.getPort() == configNodeLocation.getInternalEndPoint().getPort()
            && configNodeWrapper.getConsensusPort()
                == configNodeLocation.getConsensusEndPoint().getPort()) {
          found = true;
          break;
        }
      }
      assertTrue(found);
    }

    // check DataNode
    for (TDataNodeLocation dataNodeLocation : dataNodeList) {
      boolean found = false;
      for (DataNodeWrapper dataNodeWrapper : dataNodeWrappers) {
        if (dataNodeWrapper.getIp().equals(dataNodeLocation.getClientRpcEndPoint().getIp())
            && dataNodeWrapper.getPort() == dataNodeLocation.getClientRpcEndPoint().getPort()
            && dataNodeWrapper.getInternalPort() == dataNodeLocation.getInternalEndPoint().getPort()
            && dataNodeWrapper.getSchemaRegionConsensusPort()
                == dataNodeLocation.getSchemaRegionConsensusEndPoint().getPort()
            && dataNodeWrapper.getDataRegionConsensusPort()
                == dataNodeLocation.getDataRegionConsensusEndPoint().getPort()) {
          found = true;
          break;
        }
      }
      assertTrue(found);
    }
  }

  /** Generate a PatternTree and serialize it into a ByteBuffer */
  public static ByteBuffer generatePatternTreeBuffer(String[] paths)
      throws IllegalPathException, IOException {
    PathPatternTree patternTree = new PathPatternTree();
    for (String path : paths) {
      patternTree.appendPathPattern(new PartialPath(path));
    }
    patternTree.constructTree();

    PublicBAOS baos = new PublicBAOS();
    patternTree.serialize(baos);
    return ByteBuffer.wrap(baos.toByteArray());
  }

  public static Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> constructPartitionSlotsMap(
      String storageGroup,
      int seriesSlotStart,
      int seriesSlotEnd,
      long timeSlotStart,
      long timeSlotEnd,
      long timePartitionInterval) {
    Map<String, Map<TSeriesPartitionSlot, TTimeSlotList>> result = new HashMap<>();
    result.put(storageGroup, new HashMap<>());

    for (int i = seriesSlotStart; i < seriesSlotEnd; i++) {
      TSeriesPartitionSlot seriesPartitionSlot = new TSeriesPartitionSlot(i);
      result
          .get(storageGroup)
          .put(seriesPartitionSlot, new TTimeSlotList().setTimePartitionSlots(new ArrayList<>()));
      for (long j = timeSlotStart; j < timeSlotEnd; j++) {
        TTimePartitionSlot timePartitionSlot = new TTimePartitionSlot(j * timePartitionInterval);
        result
            .get(storageGroup)
            .get(seriesPartitionSlot)
            .getTimePartitionSlots()
            .add(timePartitionSlot);
      }
    }

    return result;
  }

  public static void checkDataPartitionTable(
      String storageGroup,
      int seriesSlotStart,
      int seriesSlotEnd,
      long timeSlotStart,
      long timeSlotEnd,
      long timePartitionInterval,
      Map<String, Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>>>
          dataPartitionTable) {

    // Check the existence of StorageGroup
    Assert.assertTrue(dataPartitionTable.containsKey(storageGroup));

    // Check the number of SeriesPartitionSlot
    Map<TSeriesPartitionSlot, Map<TTimePartitionSlot, List<TConsensusGroupId>>>
        seriesPartitionTable = dataPartitionTable.get(storageGroup);
    Assert.assertEquals(seriesSlotEnd - seriesSlotStart, seriesPartitionTable.size());

    for (int i = seriesSlotStart; i < seriesSlotEnd; i++) {
      // Check the existence of SeriesPartitionSlot
      TSeriesPartitionSlot seriesPartitionSlot = new TSeriesPartitionSlot(i);
      Assert.assertTrue(seriesPartitionTable.containsKey(seriesPartitionSlot));

      // Check the number of TimePartitionSlot
      Map<TTimePartitionSlot, List<TConsensusGroupId>> timePartitionTable =
          seriesPartitionTable.get(seriesPartitionSlot);
      Assert.assertEquals(timeSlotEnd - timeSlotStart, timePartitionTable.size());

      for (long j = timeSlotStart; j < timeSlotEnd; j++) {
        // Check the existence of TimePartitionSlot
        TTimePartitionSlot timePartitionSlot = new TTimePartitionSlot(j * timePartitionInterval);
        Assert.assertTrue(timePartitionTable.containsKey(timePartitionSlot));
      }
    }
  }
}
