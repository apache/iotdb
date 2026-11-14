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
package org.apache.iotdb.commons;

import org.apache.iotdb.commons.consensus.ConsensusProtocolUtils;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ConsensusProtocolUtilsTest {
  @Test
  public void recognizesElectedRaftProtocolsWithoutChangingWalRequirements() {
    for (String protocol :
        new String[] {
          ConsensusProtocolUtils.RAFT_CONSENSUS,
        }) {
      assertTrue(ConsensusProtocolUtils.isRaftConsensus(protocol));
      assertTrue(ConsensusProtocolUtils.isSelfRaftConsensus(protocol));
    }
    assertTrue(
        ConsensusProtocolUtils.isRaftConsensus("org.apache.iotdb.consensus.ratis.RatisConsensus"));
    assertFalse(
        ConsensusProtocolUtils.isSelfRaftConsensus(
            "org.apache.iotdb.consensus.ratis.RatisConsensus"));
  }

  @Test
  public void rejectsRoutingOnlyAndUnknownProtocols() {
    for (String protocol :
        new String[] {
          null,
          "",
          "RaftConsensus",
          "org.apache.iotdb.consensus.iot.IoTConsensus",
          "org.apache.iotdb.consensus.iot.IoTConsensusV2",
          "org.apache.iotdb.consensus.simple.SimpleConsensus"
        }) {
      assertFalse(ConsensusProtocolUtils.isRaftConsensus(protocol));
      assertFalse(ConsensusProtocolUtils.isSelfRaftConsensus(protocol));
    }
  }
}
