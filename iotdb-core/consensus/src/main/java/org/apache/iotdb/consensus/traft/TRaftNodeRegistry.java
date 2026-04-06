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

package org.apache.iotdb.consensus.traft;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for the same-JVM transport fast path.
 *
 * <p>This is not the distributed discovery mechanism. Cross-process communication goes through the
 * TRaft RPCs defined on the DataNode internal service.
 */
class TRaftNodeRegistry {

  private static final Map<String, TRaftConsensus> CONSENSUS_BY_ENDPOINT = new ConcurrentHashMap<>();

  private TRaftNodeRegistry() {}

  static void register(TEndPoint endpoint, TRaftConsensus consensus) {
    CONSENSUS_BY_ENDPOINT.put(toEndpointKey(endpoint), consensus);
  }

  static void unregister(TEndPoint endpoint) {
    CONSENSUS_BY_ENDPOINT.remove(toEndpointKey(endpoint));
  }

  static Optional<TRaftConsensus> resolveConsensus(TEndPoint endpoint) {
    return Optional.ofNullable(CONSENSUS_BY_ENDPOINT.get(toEndpointKey(endpoint)));
  }

  private static String toEndpointKey(TEndPoint endpoint) {
    return endpoint.getIp() + ":" + endpoint.getPort();
  }
}
