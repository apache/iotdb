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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.consensus.iot;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.config.ConsensusConfig;
import org.apache.iotdb.consensus.iot.log.GetConsensusReqReaderPlan;
import org.apache.iotdb.consensus.iot.util.FakeConsensusReqReader;
import org.apache.iotdb.consensus.iot.util.RequestSets;
import org.apache.iotdb.consensus.iot.util.TestStateMachine;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class IoTConsensusRecoveryTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testPeerListResetFailureAbortsRecoveryAndStopsCreatedPeer() throws Exception {
    File storageDir = temporaryFolder.newFolder("consensus");
    DataRegionId regionId = new DataRegionId(1);
    TEndPoint selfEndpoint = new TEndPoint("127.0.0.1", 6667);
    Peer self = new Peer(regionId, 0, selfEndpoint);
    IoTConsensus initialConsensus =
        newConsensus(storageDir, selfEndpoint, groupId -> new TestStateMachine());
    try {
      initialConsensus.createLocalPeer(regionId, Collections.singletonList(self));
    } finally {
      initialConsensus.stop();
    }

    AtomicInteger iteratorCount = new AtomicInteger();
    FakeConsensusReqReader reader =
        new FakeConsensusReqReader(new RequestSets(Collections.emptySet())) {
          @Override
          public ReqIterator getReqIterator(long startIndex) {
            if (iteratorCount.getAndIncrement() == 0) {
              return super.getReqIterator(startIndex);
            }
            throw new IllegalStateException("failed to restore peer list");
          }
        };
    AtomicBoolean stateMachineStopped = new AtomicBoolean();
    TestStateMachine recoveredStateMachine =
        new TestStateMachine() {
          @Override
          public synchronized DataSet read(IConsensusRequest request) {
            return request instanceof GetConsensusReqReaderPlan ? reader : null;
          }

          @Override
          public void stop() {
            stateMachineStopped.set(true);
          }
        };
    IoTConsensus recoveringConsensus =
        newConsensus(storageDir, selfEndpoint, groupId -> recoveredStateMachine);
    Peer firstPeer = new Peer(regionId, 1, new TEndPoint("127.0.0.1", 6668));
    Peer secondPeer = new Peer(regionId, 2, new TEndPoint("127.0.0.1", 6669));
    recoveringConsensus.recordCorrectPeerListBeforeStarting(
        Collections.singletonMap(regionId, Arrays.asList(self, firstPeer, secondPeer)));

    try {
      IOException failure = assertThrows(IOException.class, recoveringConsensus::start);

      assertTrue(failure.getCause() instanceof IllegalStateException);
      assertTrue(stateMachineStopped.get());
      assertTrue(recoveringConsensus.getAllConsensusGroupIds().isEmpty());
    } finally {
      recoveringConsensus.stop();
    }
  }

  private IoTConsensus newConsensus(
      File storageDir, TEndPoint selfEndpoint, IStateMachine.Registry registry) {
    ConsensusConfig config =
        ConsensusConfig.newBuilder()
            .setThisNodeId(0)
            .setThisNode(selfEndpoint)
            .setStorageDir(storageDir.getAbsolutePath())
            .setConsensusGroupType(TConsensusGroupType.DataRegion)
            .build();
    return new IoTConsensus(config, registry);
  }
}
