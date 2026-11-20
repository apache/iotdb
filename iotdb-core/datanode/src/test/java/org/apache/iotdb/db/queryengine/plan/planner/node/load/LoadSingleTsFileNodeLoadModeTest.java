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

package org.apache.iotdb.db.queryengine.plan.planner.node.load;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.partition.executor.SeriesPartitionExecutor;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.load.LoadSingleTsFileNode;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;

public class LoadSingleTsFileNodeLoadModeTest {

  @Test
  public void singleDeviceSlotAndTimeSlotUsesWholeFileConsensus() {
    final TRegionReplicaSet region = region(1, 1, 2, 3);
    final LoadSingleTsFileNode node = nodeWithDevices(0L, 1L, false, device("root.sg.d1"));

    final LoadSingleTsFileNode.LoadMode mode =
        node.resolveLoadMode(ignored -> Collections.singletonList(region));
    Assert.assertEquals(LoadSingleTsFileNode.LoadMode.CONSENSUS_WHOLE_FILE, mode);
    Assert.assertEquals(region, node.getLocalRegionReplicaSet());
    Assert.assertNotNull(node.getWholeFileTimePartitionSlot());
  }

  @Test
  public void severalDeviceSlotsInOneRegionUseWholeFileConsensus() {
    final TRegionReplicaSet region = region(1, 1, 2, 3);
    final StringArrayDeviceID first = device("root.sg.d1");
    final SeriesPartitionExecutor executor =
        SeriesPartitionExecutor.getSeriesPartitionExecutor(
            IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionExecutorClass(),
            IoTDBDescriptor.getInstance().getConfig().getSeriesPartitionSlotNum());

    StringArrayDeviceID second = null;
    for (int i = 2; i < 10_000 && second == null; i++) {
      final StringArrayDeviceID candidate = device("root.sg.d" + i);
      if (!executor
          .getSeriesPartitionSlot(first)
          .equals(executor.getSeriesPartitionSlot(candidate))) {
        second = candidate;
      }
    }
    Assert.assertNotNull(
        "the test needs two device ids that map to different series slots", second);

    final LoadSingleTsFileNode node = nodeWithDevices(0L, 100L, false, first, second);
    final LoadSingleTsFileNode.LoadMode mode =
        node.resolveLoadMode(ignored -> Collections.singletonList(region));
    Assert.assertEquals(LoadSingleTsFileNode.LoadMode.CONSENSUS_WHOLE_FILE, mode);
  }

  @Test
  public void severalTimeSlotsKeepTheSplitPath() {
    final TRegionReplicaSet region = region(1, 1, 2, 3);
    final long interval = TimePartitionUtils.getTimePartitionInterval();
    final LoadSingleTsFileNode node =
        nodeWithDevices(1L, 2 * interval, false, device("root.sg.d1"));

    final LoadSingleTsFileNode.LoadMode mode =
        node.resolveLoadMode(ignored -> Collections.singletonList(region));
    Assert.assertEquals(LoadSingleTsFileNode.LoadMode.CONSENSUS_SPLIT, mode);
  }

  @Test
  public void severalRegionsKeepTheSplitPath() {
    final TRegionReplicaSet first = region(1, 1, 2);
    final TRegionReplicaSet second = region(2, 3, 4);
    final LoadSingleTsFileNode node = nodeWithDevices(0L, 100L, false, device("root.sg.d1"));

    final LoadSingleTsFileNode.LoadMode mode =
        node.resolveLoadMode(ignored -> Arrays.asList(first, second));
    Assert.assertEquals(LoadSingleTsFileNode.LoadMode.CONSENSUS_SPLIT, mode);
  }

  @Test
  public void timeColumnConversionKeepsTheSplitPath() {
    final TRegionReplicaSet region = region(1, 1, 2, 3);
    final LoadSingleTsFileNode node = nodeWithDevices(0L, 100L, true, device("root.sg.d1"));

    final LoadSingleTsFileNode.LoadMode mode =
        node.resolveLoadMode(ignored -> Collections.singletonList(region));
    Assert.assertEquals(LoadSingleTsFileNode.LoadMode.CONSENSUS_SPLIT, mode);
  }

  private static StringArrayDeviceID device(final String deviceId) {
    return new StringArrayDeviceID(deviceId);
  }

  private static LoadSingleTsFileNode nodeWithDevices(
      final long startTime,
      final long endTime,
      final boolean needDecodeTsFile,
      final StringArrayDeviceID... devices) {
    final TsFileResource resource = new TsFileResource(new File("1.tsfile"));
    for (final StringArrayDeviceID device : devices) {
      resource.updateStartTime(device, startTime);
      resource.updateEndTime(device, endTime);
    }
    return new LoadSingleTsFileNode(
        new PlanNodeId(""), resource, false, "root.sg", false, 1L, needDecodeTsFile);
  }

  private static TRegionReplicaSet region(final int regionId, final int... dataNodeIds) {
    final TDataNodeLocation[] locations = new TDataNodeLocation[dataNodeIds.length];
    for (int i = 0; i < dataNodeIds.length; i++) {
      locations[i] =
          new TDataNodeLocation()
              .setDataNodeId(dataNodeIds[i])
              .setInternalEndPoint(new TEndPoint("127.0.0.1", 6667 + dataNodeIds[i]));
    }
    return new TRegionReplicaSet(
        new TConsensusGroupId(TConsensusGroupType.DataRegion, regionId), Arrays.asList(locations));
  }
}
