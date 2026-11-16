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

package org.apache.iotdb.db.queryengine.plan.scheduler.load;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DataPartitionBatchFetcherTest {

  private static final IDeviceID DEVICE = IDeviceID.Factory.DEFAULT_FACTORY.create("root.sg.d1");
  private static final TTimePartitionSlot SLOT = new TTimePartitionSlot(0);

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  @Test
  public void testPreferredDataNodeIsForwarded() {
    // dataNodeId is -1 before the DataNode joins, which would make this assertion vacuous
    final int originDataNodeId = config.getDataNodeId();
    config.setDataNodeId(3);
    try {
      assertEqualsPreferredDataNodeForwarded(3);
    } finally {
      config.setDataNodeId(originDataNodeId);
    }
  }

  private void assertEqualsPreferredDataNodeForwarded(final int expectedDataNodeId) {
    final IPartitionFetcher fetcher = mock(IPartitionFetcher.class);
    final DataPartition dataPartition = mock(DataPartition.class);
    when(fetcher.getOrCreateDataPartition(anyList(), anyString(), anyInt()))
        .thenReturn(dataPartition);
    when(dataPartition.getDataRegionReplicaSetForWriting(
            any(IDeviceID.class), any(TTimePartitionSlot.class), anyString()))
        .thenReturn(
            new TRegionReplicaSet()
                .setRegionId(new TConsensusGroupId(TConsensusGroupType.DataRegion, 0)));

    final DataPartitionBatchFetcher batchFetcher = new DataPartitionBatchFetcher(fetcher, true);
    batchFetcher.setDatabase("db1");
    final List<TRegionReplicaSet> replicaSets =
        batchFetcher.queryDataPartition(
            Collections.singletonList(new Pair<>(DEVICE, SLOT)), "root");

    Assert.assertEquals(1, replicaSets.size());
    // The local DataNode is the placement preference when the LOAD statement asks for it
    verify(fetcher).getOrCreateDataPartition(anyList(), eq("root"), eq(expectedDataNodeId));
  }

  @Test
  public void testPreferenceIsDisabledByStatement() {
    final IPartitionFetcher fetcher = mock(IPartitionFetcher.class);
    final DataPartition dataPartition = mock(DataPartition.class);
    when(fetcher.getOrCreateDataPartition(anyList(), anyString(), anyInt()))
        .thenReturn(dataPartition);
    when(dataPartition.getDataRegionReplicaSetForWriting(
            any(IDeviceID.class), any(TTimePartitionSlot.class), anyString()))
        .thenReturn(
            new TRegionReplicaSet()
                .setRegionId(new TConsensusGroupId(TConsensusGroupType.DataRegion, 0)));

    final DataPartitionBatchFetcher batchFetcher = new DataPartitionBatchFetcher(fetcher, false);
    batchFetcher.setDatabase("db1");
    batchFetcher.queryDataPartition(Collections.singletonList(new Pair<>(DEVICE, SLOT)), "root");

    // -1 keeps the allocation plan free of any placement preference
    verify(fetcher).getOrCreateDataPartition(anyList(), eq("root"), eq(-1));
  }
}
