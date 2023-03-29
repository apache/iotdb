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

package org.apache.iotdb.db.mpp.execution.exchange;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeMPPDataExchangeServiceClient;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.execution.exchange.sink.DownStreamChannelIndex;
import org.apache.iotdb.db.mpp.execution.exchange.sink.DownStreamChannelLocation;
import org.apache.iotdb.db.mpp.execution.exchange.sink.ISinkHandle;
import org.apache.iotdb.db.mpp.execution.exchange.sink.LocalSinkChannel;
import org.apache.iotdb.db.mpp.execution.exchange.sink.ShuffleSinkHandle;
import org.apache.iotdb.db.mpp.execution.exchange.source.ISourceHandle;
import org.apache.iotdb.db.mpp.execution.exchange.source.LocalSourceHandle;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.mpp.execution.memory.LocalMemoryManager;
import org.apache.iotdb.db.mpp.execution.memory.MemoryPool;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.concurrent.Executors;

public class MPPDataExchangeManagerTest {
  @Test
  public void testCreateLocalSinkHandle() {
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId("q0", 1, "0");
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId("q0", 0, "0");
    final String remotePlanNodeId = "exchange_0";
    final String localPlanNodeId = "shuffleSink_0";
    final FragmentInstanceContext mockFragmentInstanceContext =
        Mockito.mock(FragmentInstanceContext.class);

    // Construct a mock LocalMemoryManager with capacity 5 * mockTsBlockSize per query.
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryPool spyMemoryPool = Mockito.spy(new MemoryPool("test", 10240L, 5120L));
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(spyMemoryPool);

    MPPDataExchangeManager mppDataExchangeManager =
        new MPPDataExchangeManager(
            mockLocalMemoryManager,
            new TsBlockSerdeFactory(),
            Executors.newSingleThreadExecutor(),
            new IClientManager.Factory<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient>()
                .createClientManager(
                    new ClientPoolFactory.SyncDataNodeMPPDataExchangeServiceClientPoolFactory()));

    ISinkHandle shuffleSinkHandle =
        mppDataExchangeManager.createShuffleSinkHandle(
            Collections.singletonList(
                new DownStreamChannelLocation(
                    new TEndPoint(
                        IoTDBDescriptor.getInstance().getConfig().getInternalAddress(),
                        IoTDBDescriptor.getInstance().getConfig().getMppDataExchangePort()),
                    remoteFragmentInstanceId,
                    remotePlanNodeId)),
            new DownStreamChannelIndex(0),
            ShuffleSinkHandle.ShuffleStrategyEnum.PLAIN,
            localFragmentInstanceId,
            localPlanNodeId,
            mockFragmentInstanceContext);

    Assert.assertTrue(shuffleSinkHandle instanceof ShuffleSinkHandle);

    ISourceHandle localSourceHandle =
        mppDataExchangeManager.createLocalSourceHandleForFragment(
            remoteFragmentInstanceId,
            remotePlanNodeId,
            localPlanNodeId,
            localFragmentInstanceId,
            0,
            t -> {});

    Assert.assertTrue(localSourceHandle instanceof LocalSourceHandle);

    Assert.assertEquals(
        ((LocalSinkChannel) shuffleSinkHandle.getChannel(0)).getSharedTsBlockQueue(),
        ((LocalSourceHandle) localSourceHandle).getSharedTsBlockQueue());
  }

  @Test
  public void testCreateLocalSourceHandle() {
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId("q0", 1, "0");
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId("q0", 0, "0");
    final String remotePlanNodeId = "exchange_0";
    final String localPlanNodeId = "shuffleSink_0";
    final FragmentInstanceContext mockFragmentInstanceContext =
        Mockito.mock(FragmentInstanceContext.class);

    // Construct a mock LocalMemoryManager with capacity 5 * mockTsBlockSize per query.
    LocalMemoryManager mockLocalMemoryManager = Mockito.mock(LocalMemoryManager.class);
    MemoryPool spyMemoryPool = Mockito.spy(new MemoryPool("test", 10240L, 5120L));
    Mockito.when(mockLocalMemoryManager.getQueryPool()).thenReturn(spyMemoryPool);

    MPPDataExchangeManager mppDataExchangeManager =
        new MPPDataExchangeManager(
            mockLocalMemoryManager,
            new TsBlockSerdeFactory(),
            Executors.newSingleThreadExecutor(),
            new IClientManager.Factory<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient>()
                .createClientManager(
                    new ClientPoolFactory.SyncDataNodeMPPDataExchangeServiceClientPoolFactory()));

    ISourceHandle localSourceHandle =
        mppDataExchangeManager.createLocalSourceHandleForFragment(
            remoteFragmentInstanceId,
            remotePlanNodeId,
            localPlanNodeId,
            localFragmentInstanceId,
            0,
            t -> {});

    Assert.assertTrue(localSourceHandle instanceof LocalSourceHandle);

    ISinkHandle shuffleSinkHandle =
        mppDataExchangeManager.createShuffleSinkHandle(
            Collections.singletonList(
                new DownStreamChannelLocation(
                    new TEndPoint(
                        IoTDBDescriptor.getInstance().getConfig().getInternalAddress(),
                        IoTDBDescriptor.getInstance().getConfig().getMppDataExchangePort()),
                    remoteFragmentInstanceId,
                    remotePlanNodeId)),
            new DownStreamChannelIndex(0),
            ShuffleSinkHandle.ShuffleStrategyEnum.PLAIN,
            localFragmentInstanceId,
            localPlanNodeId,
            mockFragmentInstanceContext);

    Assert.assertTrue(shuffleSinkHandle instanceof ShuffleSinkHandle);

    Assert.assertEquals(
        ((LocalSinkChannel) shuffleSinkHandle.getChannel(0)).getSharedTsBlockQueue(),
        ((LocalSourceHandle) localSourceHandle).getSharedTsBlockQueue());
  }
}
