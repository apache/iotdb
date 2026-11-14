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
package org.apache.iotdb.commons.consensus;

/** Protocol capabilities shared by ConfigNode and DataNode without loading consensus classes. */
public final class ConsensusProtocolUtils {

  public static final String RAFT_CONSENSUS = "org.apache.iotdb.consensus.raft.RaftConsensus";

  public static final String T_RAFT_CONSENSUS = "org.apache.iotdb.consensus.traft.TRaftConsensus";

  private ConsensusProtocolUtils() {}

  /** Whether leadership is elected by a Raft group rather than assigned by ConfigNode routing. */
  public static boolean isRaftConsensus(String protocolClass) {
    return "org.apache.iotdb.consensus.ratis.RatisConsensus".equals(protocolClass)
        || isSelfRaftConsensus(protocolClass);
  }

  /** Research Raft backends restore regions from consensus logs and portable snapshots. */
  public static boolean isSelfRaftConsensus(String protocolClass) {
    return RAFT_CONSENSUS.equals(protocolClass) || T_RAFT_CONSENSUS.equals(protocolClass);
  }
}
