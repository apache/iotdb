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

package org.apache.iotdb.consensus.standalone;

import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.common.ConsensusGroupId;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Endpoint;
import org.apache.iotdb.consensus.common.GroupType;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.response.ConsensusGenericResponse;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.consensus.exception.ConsensusGroupAlreadyExistException;
import org.apache.iotdb.consensus.exception.ConsensusGroupNotExistException;
import org.apache.iotdb.consensus.exception.IllegalPeerNumException;
import org.apache.iotdb.consensus.statemachine.EmptyStateMachine;
import org.apache.iotdb.consensus.statemachine.IStateMachine;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class StandAloneConsensusTest {

  private IConsensus consensusImpl;
  private final TestEntry entry = new TestEntry(0);
  private final ConsensusGroupId dataRegionId = new ConsensusGroupId(GroupType.DataRegion, 0);
  private final ConsensusGroupId schemaRegionId = new ConsensusGroupId(GroupType.SchemaRegion, 1);
  private final ConsensusGroupId configId = new ConsensusGroupId(GroupType.Config, 2);

  private static class TestEntry implements IConsensusRequest {

    private int num;

    public TestEntry(int num) {
      this.num = num;
    }

    @Override
    public void serializeRequest(ByteBuffer buffer) {
      buffer.putInt(num);
    }

    @Override
    public void deserializeRequest(ByteBuffer buffer) throws Exception {
      num = buffer.getInt();
    }
  }

  private static class TestStateMachine implements IStateMachine {

    private final boolean direction;

    public TestStateMachine(boolean direction) {
      this.direction = direction;
    }

    @Override
    public void start() {}

    @Override
    public void stop() {}

    @Override
    public TSStatus Write(IConsensusRequest request) {
      if (request instanceof TestEntry) {
        return new TSStatus(
            direction ? ((TestEntry) request).num + 1 : ((TestEntry) request).num - 1);
      }
      return new TSStatus();
    }

    @Override
    public DataSet Read(IConsensusRequest request) {
      return null;
    }
  }

  @Before
  public void setUp() throws Exception {
    consensusImpl =
        new StandAloneConsensus(
            gid -> {
              switch (gid.getType()) {
                case SchemaRegion:
                  return new TestStateMachine(true);
                case DataRegion:
                  return new TestStateMachine(false);
              }
              return new EmptyStateMachine();
            });
    consensusImpl.start();
  }

  @After
  public void tearDown() throws Exception {
    consensusImpl.stop();
  }

  @Test
  public void addConsensusGroup() {
    ConsensusGenericResponse response1 =
        consensusImpl.AddConsensusGroup(
            dataRegionId,
            Collections.singletonList(new Peer(dataRegionId, new Endpoint("0.0.0.0", 6667))));
    assertTrue(response1.isSuccess());
    assertNull(response1.getException());

    ConsensusGenericResponse response2 =
        consensusImpl.AddConsensusGroup(
            dataRegionId,
            Collections.singletonList(new Peer(dataRegionId, new Endpoint("0.0.0.0", 6667))));
    assertFalse(response2.isSuccess());
    assertTrue(response2.getException() instanceof ConsensusGroupAlreadyExistException);

    ConsensusGenericResponse response3 =
        consensusImpl.AddConsensusGroup(
            dataRegionId,
            Arrays.asList(
                new Peer(dataRegionId, new Endpoint("0.0.0.0", 6667)),
                new Peer(dataRegionId, new Endpoint("0.0.0.1", 6667))));
    assertFalse(response3.isSuccess());
    assertTrue(response3.getException() instanceof IllegalPeerNumException);

    ConsensusGenericResponse response4 =
        consensusImpl.AddConsensusGroup(
            schemaRegionId,
            Collections.singletonList(new Peer(schemaRegionId, new Endpoint("0.0.0.0", 6667))));
    assertTrue(response4.isSuccess());
    assertNull(response4.getException());
  }

  @Test
  public void removeConsensusGroup() {
    ConsensusGenericResponse response1 = consensusImpl.RemoveConsensusGroup(dataRegionId);
    assertFalse(response1.isSuccess());
    assertTrue(response1.getException() instanceof ConsensusGroupNotExistException);

    ConsensusGenericResponse response2 =
        consensusImpl.AddConsensusGroup(
            dataRegionId,
            Collections.singletonList(new Peer(dataRegionId, new Endpoint("0.0.0.0", 6667))));
    assertTrue(response2.isSuccess());
    assertNull(response2.getException());

    ConsensusGenericResponse response3 = consensusImpl.RemoveConsensusGroup(dataRegionId);
    assertTrue(response3.isSuccess());
    assertNull(response3.getException());
  }

  @Test
  public void addPeer() {
    ConsensusGenericResponse response =
        consensusImpl.AddPeer(dataRegionId, new Peer(dataRegionId, new Endpoint("0.0.0.0", 6667)));
    assertFalse(response.isSuccess());
  }

  @Test
  public void removePeer() {
    ConsensusGenericResponse response =
        consensusImpl.RemovePeer(
            dataRegionId, new Peer(dataRegionId, new Endpoint("0.0.0.0", 6667)));
    assertFalse(response.isSuccess());
  }

  @Test
  public void transferLeader() {
    ConsensusGenericResponse response =
        consensusImpl.TransferLeader(
            dataRegionId, new Peer(dataRegionId, new Endpoint("0.0.0.0", 6667)));
    assertFalse(response.isSuccess());
  }

  @Test
  public void triggerSnapshot() {
    ConsensusGenericResponse response = consensusImpl.TriggerSnapshot(dataRegionId);
    assertFalse(response.isSuccess());
  }

  @Test
  public void write() {
    ConsensusGenericResponse response1 =
        consensusImpl.AddConsensusGroup(
            dataRegionId,
            Collections.singletonList(new Peer(dataRegionId, new Endpoint("0.0.0.0", 6667))));
    assertTrue(response1.isSuccess());
    assertNull(response1.getException());

    ConsensusGenericResponse response2 =
        consensusImpl.AddConsensusGroup(
            schemaRegionId,
            Collections.singletonList(new Peer(schemaRegionId, new Endpoint("0.0.0.0", 6667))));
    assertTrue(response2.isSuccess());
    assertNull(response2.getException());

    ConsensusGenericResponse response3 =
        consensusImpl.AddConsensusGroup(
            configId, Collections.singletonList(new Peer(configId, new Endpoint("0.0.0.0", 6667))));
    assertTrue(response3.isSuccess());
    assertNull(response3.getException());

    ConsensusWriteResponse response4 = consensusImpl.Write(dataRegionId, entry);
    assertNull(response4.getException());
    assertNotNull(response4.getStatus());
    assertEquals(-1, response4.getStatus().getCode());

    ConsensusWriteResponse response5 = consensusImpl.Write(schemaRegionId, entry);
    assertNull(response5.getException());
    assertNotNull(response5.getStatus());
    assertEquals(1, response5.getStatus().getCode());

    ConsensusWriteResponse response6 = consensusImpl.Write(configId, entry);
    assertNull(response6.getException());
    assertEquals(0, response6.getStatus().getCode());
  }
}
