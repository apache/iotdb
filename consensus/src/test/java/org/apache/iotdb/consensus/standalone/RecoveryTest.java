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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.response.ConsensusGenericResponse;
import org.apache.iotdb.consensus.exception.ConsensusGroupAlreadyExistException;
import org.apache.iotdb.consensus.statemachine.EmptyStateMachine;

import org.apache.ratis.util.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

public class RecoveryTest {
  private final ConsensusGroupId schemaRegionId = new SchemaRegionId(1);
  private IConsensus consensusImpl;
  private static final String STANDALONE_CONSENSUS_CLASS_NAME =
      "org.apache.iotdb.consensus.standalone.StandAloneConsensus";

  public void constructConsensus() throws IOException {
    consensusImpl =
        ConsensusFactory.getConsensusImpl(
                STANDALONE_CONSENSUS_CLASS_NAME,
                new TEndPoint("localhost", 9000),
                new File("./target/recovery"),
                gid -> new EmptyStateMachine())
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        String.format(
                            ConsensusFactory.CONSTRUCT_FAILED_MSG,
                            STANDALONE_CONSENSUS_CLASS_NAME)));
    consensusImpl.start();
  }

  @Before
  public void setUp() throws Exception {
    constructConsensus();
  }

  @After
  public void tearDown() throws IOException {
    FileUtils.deleteFully(new File("./target/recovery"));
  }

  @Test
  public void recoveryTest() throws Exception {
    consensusImpl.addConsensusGroup(
        schemaRegionId,
        Collections.singletonList(new Peer(schemaRegionId, new TEndPoint("0.0.0.0", 9000))));

    consensusImpl.stop();
    consensusImpl = null;

    constructConsensus();

    ConsensusGenericResponse response =
        consensusImpl.addConsensusGroup(
            schemaRegionId,
            Collections.singletonList(new Peer(schemaRegionId, new TEndPoint("0.0.0.0", 9000))));

    Assert.assertEquals(
        response.getException().getMessage(),
        new ConsensusGroupAlreadyExistException(schemaRegionId).getMessage());
  }
}
