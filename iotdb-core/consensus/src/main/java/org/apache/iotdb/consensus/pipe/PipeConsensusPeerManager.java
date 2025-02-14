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

import org.apache.iotdb.consensus.common.Peer;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class PipeConsensusPeerManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeConsensusPeerManager.class);

  private final Set<Peer> peers;

  public PipeConsensusPeerManager(List<Peer> peers) {
    this.peers = Collections.newSetFromMap(new ConcurrentHashMap<>());

    this.peers.addAll(peers);
    if (this.peers.size() != peers.size()) {
      LOGGER.warn("Duplicate peers in the input list, ignore the duplicates.");
    }
  }

  public boolean contains(Peer peer) {
    return peers.contains(peer);
  }

  public void addPeer(Peer peer) {
    peers.add(peer);
  }

  public void removePeer(Peer peer) {
    peers.remove(peer);
  }

  public List<Peer> getOtherPeers(Peer thisNode) {
    return peers.stream()
        .filter(peer -> !peer.equals(thisNode))
        .collect(ImmutableList.toImmutableList());
  }

  public List<Peer> getPeers() {
    return ImmutableList.copyOf(peers);
  }

  public void clear() {
    peers.clear();
  }
}
