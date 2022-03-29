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

package org.apache.iotdb.consensus.common;

import java.util.List;
import java.util.Objects;

// TODO Use a mature IDL framework such as Protobuf to manage this structure
public class ConsensusGroup {

  private final ConsensusGroupId groupId;
  private final List<Peer> peers;

  public ConsensusGroup(ConsensusGroupId groupId, List<Peer> peers) {
    this.groupId = groupId;
    this.peers = peers;
  }

  public ConsensusGroupId getGroupId() {
    return groupId;
  }

  public List<Peer> getPeers() {
    return peers;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ConsensusGroup that = (ConsensusGroup) o;
    return Objects.equals(groupId, that.groupId) && Objects.equals(peers, that.peers);
  }

  @Override
  public int hashCode() {
    return Objects.hash(groupId, peers);
  }
}
