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

package org.apache.iotdb.cluster.client;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientPool {

  private static final Logger logger = LoggerFactory.getLogger(ClientPool.class);
  private Map<Node, Deque<AsyncClient>> clientCaches = new ConcurrentHashMap<>();
  private ClientFactory clientFactory;

  public ClientPool(ClientFactory clientFactory) {
    this.clientFactory = clientFactory;
  }

  /**
   * Get a client of the given node from the cache if one is available, or create a new one.
   * @param node
   * @return
   * @throws IOException
   */
  public AsyncClient getClient(Node node) throws IOException {
    //As clientCaches is ConcurrentHashMap, computeIfAbsent is thread safety.
    Deque<AsyncClient> clientStack = clientCaches.computeIfAbsent(node, n -> new ArrayDeque<>());
    //TODO this pool may increase infinitely.
    synchronized (clientStack) {
      if (clientStack.isEmpty()) {
        return clientFactory.getAsyncClient(node, this);
      } else {
        return clientStack.pop();
      }
    }
  }

  /**
   * Return a client of a node to the pool. Closed client should not be returned.
   * @param node
   * @param client
   */
  public void putClient(Node node, AsyncClient client) {
    //As clientCaches is ConcurrentHashMap, computeIfAbsent is thread safety.
    Deque<AsyncClient> clientStack = clientCaches.computeIfAbsent(node, n -> new ArrayDeque<>());
    synchronized (clientStack) {
      clientStack.push(client);
    }
  }
}
