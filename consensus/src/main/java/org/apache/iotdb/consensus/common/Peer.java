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

import java.util.Objects;

// TODO Use a mature IDL framework such as Protobuf to manage this structure
public class Peer {

  private final ConsensusGroupId groupId;
  private final Endpoint endpoint;

  public Peer(ConsensusGroupId groupId, Endpoint endpoint) {
    this.groupId = groupId;
    this.endpoint = endpoint;
  }

  public ConsensusGroupId getGroupId() {
    return groupId;
  }

  public Endpoint getEndpoint() {
    return endpoint;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Peer peer = (Peer) o;
    return Objects.equals(groupId, peer.groupId) && Objects.equals(endpoint, peer.endpoint);
  }

  @Override
  public int hashCode() {
    return Objects.hash(groupId, endpoint);
  }
}
