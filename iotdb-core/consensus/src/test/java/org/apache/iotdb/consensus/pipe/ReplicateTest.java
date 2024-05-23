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

package org.apache.iotdb.consensus.pipe;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.common.ConsensusGroup;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.config.ConsensusConfig;
import org.apache.iotdb.consensus.iot.IoTConsensus;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ReplicateTest {
  private final Logger logger = LoggerFactory.getLogger(ReplicateTest.class);
  private static final String CONFIGURATION_FILE_NAME = "configuration.dat";
  private static final String CONFIGURATION_TMP_FILE_NAME = "configuration.dat.tmp";
  private static final long timeout = TimeUnit.SECONDS.toMillis(300);
  private final ConsensusGroupId gid = new DataRegionId(1);
  private int basePort = 9000;
  private final List<Peer> peers =
      Arrays.asList(
          new Peer(gid, 1, new TEndPoint("127.0.0.1", basePort - 2)),
          new Peer(gid, 2, new TEndPoint("127.0.0.1", basePort - 1)),
          new Peer(gid, 3, new TEndPoint("127.0.0.1", basePort)));
  private final ConsensusGroup group = new ConsensusGroup(gid, peers);
  private final List<PipeConsensus> servers = new ArrayList<>();

  @Before
  public void setUp() throws Exception {
    for (int i = 0; i < peers.size(); i++) {
      findPortAvailable(i);
    }
    for (int i = 0; i < peers.size(); i++) {
      int finalI = i;
      changeConfiguration(i);
      servers.add(
          (IoTConsensus)
              ConsensusFactory.getConsensusImpl(
                      ConsensusFactory.PIPE_CONSENSUS,
                      ConsensusConfig.newBuilder()
                          .setThisNodeId(peers.get(i).getNodeId())
                          .setThisNode(peers.get(i).getEndpoint())
                          .setConsensusGroupType(TConsensusGroupType.DataRegion)
                          .build(),
                      groupId -> stateMachines.get(finalI))
                  .orElseThrow(
                      () ->
                          new IllegalArgumentException(
                              String.format(
                                  ConsensusFactory.CONSTRUCT_FAILED_MSG,
                                  ConsensusFactory.IOT_CONSENSUS))));
      servers.get(i).start();
    }
  }

  @After
  public void tearDown() throws Exception {}

  private void findPortAvailable(int i) {
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < timeout) {
      try (ServerSocket ignored = new ServerSocket(this.peers.get(i).getEndpoint().port)) {
        // success
        return;
      } catch (IOException e) {
        // Port is already in use, wait and retry
        this.peers.set(i, new Peer(gid, i + 1, new TEndPoint("127.0.0.1", this.basePort)));
        logger.info("try port {} for node {}.", this.basePort++, i + 1);
        try {
          Thread.sleep(50); // Wait for 1 second before retrying
        } catch (InterruptedException ex) {
          // Handle the interruption if needed
        }
      }
    }
    Assert.fail(String.format("can not find port for node %d after 300s", i + 1));
  }
}
