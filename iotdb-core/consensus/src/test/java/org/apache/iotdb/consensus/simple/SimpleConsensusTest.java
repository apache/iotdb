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

package org.apache.iotdb.consensus.simple;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.EmptyStateMachine;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.ByteBufferConsensusRequest;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.response.ConsensusGenericResponse;
import org.apache.iotdb.consensus.config.ConsensusConfig;
import org.apache.iotdb.consensus.exception.*;

import org.apache.ratis.util.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SimpleConsensusTest {

  private IConsensus consensusImpl;
  private final TestEntry entry1 = new TestEntry(0);
  private final ByteBufferConsensusRequest entry2 =
      new ByteBufferConsensusRequest(ByteBuffer.wrap(new byte[4]));
  private final ConsensusGroupId dataRegionId = new DataRegionId(0);
  private final ConsensusGroupId schemaRegionId = new SchemaRegionId(1);
  private final ConsensusGroupId configId = new ConfigRegionId(2);

  private static class TestEntry implements IConsensusRequest {

    private final int num;

    public TestEntry(int num) {
      this.num = num;
    }

    @Override
    public ByteBuffer serializeToByteBuffer() {
      ByteBuffer buffer = ByteBuffer.allocate(4).putInt(num);
      buffer.flip();
      return buffer;
    }
  }

  private static class TestStateMachine implements IStateMachine, IStateMachine.EventApi {

    private final boolean direction;

    public TestStateMachine(boolean direction) {
      this.direction = direction;
    }

    @Override
    public void start() {}

    @Override
    public void stop() {}

    @Override
    public TSStatus write(IConsensusRequest request) {
      if (request instanceof ByteBufferConsensusRequest) {
        return new TSStatus(request.serializeToByteBuffer().getInt());
      } else if (request instanceof TestEntry) {
        return new TSStatus(
            direction ? ((TestEntry) request).num + 1 : ((TestEntry) request).num - 1);
      }
      return new TSStatus();
    }

    @Override
    public IConsensusRequest deserializeRequest(IConsensusRequest request) {
      return request;
    }

    @Override
    public DataSet read(IConsensusRequest request) {
      return null;
    }

    @Override
    public boolean takeSnapshot(File snapshotDir) {
      return false;
    }

    @Override
    public void loadSnapshot(File latestSnapshotRootDir) {}
  }

  @Before
  public void setUp() throws Exception {
    consensusImpl =
        ConsensusFactory.getConsensusImpl(
                ConsensusFactory.SIMPLE_CONSENSUS,
                ConsensusConfig.newBuilder()
                    .setThisNodeId(1)
                    .setThisNode(new TEndPoint("0.0.0.0", 6667))
                    .setStorageDir("target" + java.io.File.separator + "standalone")
                    .setConsensusGroupType(TConsensusGroupType.DataRegion)
                    .build(),
                gid -> {
                  switch (gid.getType()) {
                    case SchemaRegion:
                      return new TestStateMachine(true);
                    case DataRegion:
                      return new TestStateMachine(false);
                    default:
                      return new EmptyStateMachine();
                  }
                })
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        String.format(
                            ConsensusFactory.CONSTRUCT_FAILED_MSG,
                            ConsensusFactory.SIMPLE_CONSENSUS)));
    consensusImpl.start();
  }

  @After
  public void tearDown() throws Exception {
    consensusImpl.stop();
    FileUtils.deleteFully(new File("./target/standalone"));
  }

  @Test
  public void addConsensusGroup() {
    try {
      consensusImpl.createLocalPeer(
          dataRegionId,
          Collections.singletonList(new Peer(dataRegionId, 1, new TEndPoint("0.0.0.0", 6667))));
    } catch (ConsensusException e) {
      throw new RuntimeException(e);
    }

    try {
      consensusImpl.createLocalPeer(
          dataRegionId,
          Collections.singletonList(new Peer(dataRegionId, 1, new TEndPoint("0.0.0.0", 6667))));
    } catch (ConsensusException e) {
      assertTrue(e instanceof ConsensusGroupAlreadyExistException);
    }

    try {
      consensusImpl.createLocalPeer(
          dataRegionId,
          Arrays.asList(
              new Peer(dataRegionId, 1, new TEndPoint("0.0.0.0", 6667)),
              new Peer(dataRegionId, 1, new TEndPoint("0.0.0.1", 6667))));
    } catch (ConsensusException e) {
      assertTrue(e instanceof IllegalPeerNumException);
    }

    try {
      consensusImpl.createLocalPeer(
          dataRegionId,
          Collections.singletonList(new Peer(dataRegionId, 1, new TEndPoint("0.0.0.1", 6667))));
    } catch (ConsensusException e) {
      assertTrue(e instanceof IllegalPeerEndpointException);
    }

    try {
      consensusImpl.createLocalPeer(
          schemaRegionId,
          Collections.singletonList(new Peer(schemaRegionId, 1, new TEndPoint("0.0.0.0", 6667))));
    } catch (ConsensusException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void removeConsensusGroup() {
    ConsensusGenericResponse response1 = consensusImpl.deleteLocalPeer(dataRegionId);
    assertFalse(response1.isSuccess());
    assertTrue(response1.getException() instanceof ConsensusGroupNotExistException);

    try {
      consensusImpl.createLocalPeer(
          dataRegionId,
          Collections.singletonList(new Peer(dataRegionId, 1, new TEndPoint("0.0.0.0", 6667))));
    } catch (ConsensusException e) {
      throw new RuntimeException(e);
    }

    ConsensusGenericResponse response3 = consensusImpl.deleteLocalPeer(dataRegionId);
    assertTrue(response3.isSuccess());
    assertNull(response3.getException());
  }

  @Test
  public void addPeer() {
    ConsensusGenericResponse response =
        consensusImpl.addRemotePeer(
            dataRegionId, new Peer(dataRegionId, 1, new TEndPoint("0.0.0.0", 6667)));
    assertFalse(response.isSuccess());
  }

  @Test
  public void removePeer() {
    ConsensusGenericResponse response =
        consensusImpl.removeRemotePeer(
            dataRegionId, new Peer(dataRegionId, 1, new TEndPoint("0.0.0.0", 6667)));
    assertFalse(response.isSuccess());
  }

  @Test
  public void transferLeader() {
    ConsensusGenericResponse response =
        consensusImpl.transferLeader(
            dataRegionId, new Peer(dataRegionId, 1, new TEndPoint("0.0.0.0", 6667)));
    assertFalse(response.isSuccess());
  }

  @Test
  public void triggerSnapshot() {
    ConsensusGenericResponse response = consensusImpl.triggerSnapshot(dataRegionId);
    assertFalse(response.isSuccess());
  }

  @Test
  public void write() {
    try {
      consensusImpl.createLocalPeer(
          dataRegionId,
          Collections.singletonList(new Peer(dataRegionId, 1, new TEndPoint("0.0.0.0", 6667))));
    } catch (ConsensusException e) {
      throw new RuntimeException(e);
    }

    try {
      consensusImpl.createLocalPeer(
          schemaRegionId,
          Collections.singletonList(new Peer(schemaRegionId, 1, new TEndPoint("0.0.0.0", 6667))));
    } catch (ConsensusException e) {
      throw new RuntimeException(e);
    }

    try {
      consensusImpl.createLocalPeer(
          configId,
          Collections.singletonList(new Peer(configId, 1, new TEndPoint("0.0.0.0", 6667))));
    } catch (ConsensusException e) {
      throw new RuntimeException(e);
    }

    // test new TestStateMachine(true), should return 1;
    try {
      TSStatus response4 = consensusImpl.write(dataRegionId, entry1);
      assertNotNull(response4);
      assertEquals(-1, response4.getCode());
    } catch (ConsensusException e) {
      throw new RuntimeException(e);
    }

    // test new TestStateMachine(false), should return -1;
    try {
      TSStatus response5 = consensusImpl.write(schemaRegionId, entry1);
      assertNotNull(response5);
      assertEquals(1, response5.getCode());
    } catch (ConsensusException e) {
      throw new RuntimeException(e);
    }

    // test new EmptyStateMachine(), should return 0;
    try {
      TSStatus response6 = consensusImpl.write(configId, entry1);
      assertNull(response6);
      assertEquals(0, response6.getCode());
    } catch (ConsensusException e) {
      throw new RuntimeException(e);
    }

    // test ByteBufferConsensusRequest, should return 0;
    try {
      TSStatus response7 = consensusImpl.write(dataRegionId, entry2);
      assertNull(response7);
      assertEquals(0, response7.getCode());
    } catch (ConsensusException e) {
      throw new RuntimeException(e);
    }
  }
}
