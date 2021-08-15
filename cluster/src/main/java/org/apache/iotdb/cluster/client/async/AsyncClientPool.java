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

package org.apache.iotdb.cluster.client.async;

import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.server.monitor.NodeStatusManager;
import org.apache.iotdb.cluster.utils.ClusterNode;
import org.apache.iotdb.db.utils.TestOnly;

import org.apache.thrift.async.TAsyncMethodCall;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AsyncClientPool {

  private static final Logger logger = LoggerFactory.getLogger(AsyncClientPool.class);
  private long waitClientTimeutMS;
  private int maxConnectionForEachNode;
  private Map<ClusterNode, Deque<AsyncClient>> clientCaches = new ConcurrentHashMap<>();
  private Map<ClusterNode, Integer> nodeClientNumMap = new ConcurrentHashMap<>();
  private AsyncClientFactory asyncClientFactory;

  public AsyncClientPool(AsyncClientFactory asyncClientFactory) {
    this.asyncClientFactory = asyncClientFactory;
    this.waitClientTimeutMS = ClusterDescriptor.getInstance().getConfig().getWaitClientTimeoutMS();
    this.maxConnectionForEachNode =
        ClusterDescriptor.getInstance().getConfig().getMaxClientPerNodePerMember();
  }

  /**
   * See getClient(Node node, boolean activatedOnly)
   *
   * @param node
   * @return
   * @throws IOException
   */
  public AsyncClient getClient(Node node) throws IOException {
    return getClient(node, true);
  }

  /**
   * Get a client of the given node from the cache if one is available, or create a new one.
   *
   * <p>IMPORTANT!!! The caller should check whether the return value is null or not!
   *
   * @param node the node want to connect
   * @param activatedOnly if true, only return a client if the node's NodeStatus.isActivated ==
   *     true, which avoid unnecessary wait for already down nodes, but heartbeat attempts should
   *     always try to connect so the node can be reactivated ASAP
   * @return if the node can connect, return the client, otherwise null
   * @throws IOException if the node can not be connected
   */
  public AsyncClient getClient(Node node, boolean activatedOnly) throws IOException {
    ClusterNode clusterNode = new ClusterNode(node);
    if (activatedOnly && !NodeStatusManager.getINSTANCE().isActivated(node)) {
      return null;
    }

    AsyncClient client;
    // As clientCaches is ConcurrentHashMap, computeIfAbsent is thread safety.
    Deque<AsyncClient> clientStack =
        clientCaches.computeIfAbsent(clusterNode, n -> new ArrayDeque<>());
    synchronized (clientStack) {
      if (clientStack.isEmpty()) {
        int nodeClientNum = nodeClientNumMap.getOrDefault(clusterNode, 0);
        if (nodeClientNum >= maxConnectionForEachNode) {
          client = waitForClient(clientStack, clusterNode);
        } else {
          client = asyncClientFactory.getAsyncClient(clusterNode, this);
          nodeClientNumMap.compute(
              clusterNode,
              (n, oldValue) -> {
                if (oldValue == null) return 1;
                return oldValue + 1;
              });
        }
      } else {
        client = clientStack.pop();
      }
    }
    return client;
  }

  /**
   * Wait for a client to be returned for at most WAIT_CLIENT_TIMEOUT_MS milliseconds. If no client
   * is returned beyond the timeout, a new client will be returned. WARNING: the caller must
   * synchronize on the pool.
   *
   * @param clientStack
   * @param clusterNode
   * @return
   * @throws IOException
   */
  @SuppressWarnings({"squid:S2273"}) // synchronized outside
  private AsyncClient waitForClient(Deque<AsyncClient> clientStack, ClusterNode clusterNode)
      throws IOException {
    // wait for an available client
    long waitStart = System.currentTimeMillis();
    while (clientStack.isEmpty()) {
      try {
        clientStack.wait(waitClientTimeutMS);
        if (clientStack.isEmpty() && System.currentTimeMillis() - waitStart >= waitClientTimeutMS) {
          logger.warn(
              "{} Cannot get an available client after {}ms, create a new one.",
              asyncClientFactory,
              waitClientTimeutMS);
          AsyncClient asyncClient = asyncClientFactory.getAsyncClient(clusterNode, this);
          nodeClientNumMap.computeIfPresent(clusterNode, (n, oldValue) -> oldValue + 1);
          return asyncClient;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn("Interrupted when waiting for an available client of {}", clusterNode);
        return null;
      }
    }
    return clientStack.pop();
  }

  /**
   * Return a client of a node to the pool. Closed client should not be returned.
   *
   * @param node
   * @param client
   */
  public void putClient(Node node, AsyncClient client) {
    ClusterNode clusterNode = new ClusterNode(node);
    TAsyncMethodCall<?> call = null;
    if (client instanceof AsyncDataClient) {
      call = ((AsyncDataClient) client).getCurrMethod();
    } else if (client instanceof AsyncMetaClient) {
      call = ((AsyncMetaClient) client).getCurrMethod();
    }
    if (call != null) {
      logger.warn("A using client {} is put back while running {}", client.hashCode(), call);
    }
    // As clientCaches is ConcurrentHashMap, computeIfAbsent is thread safety.
    Deque<AsyncClient> clientStack =
        clientCaches.computeIfAbsent(clusterNode, n -> new ArrayDeque<>());
    synchronized (clientStack) {
      clientStack.push(client);
      clientStack.notifyAll();
    }
  }

  void onError(Node node) {
    ClusterNode clusterNode = new ClusterNode(node);
    // clean all cached clients when network fails
    Deque<AsyncClient> clientStack =
        clientCaches.computeIfAbsent(clusterNode, n -> new ArrayDeque<>());
    synchronized (clientStack) {
      while (!clientStack.isEmpty()) {
        AsyncClient client = clientStack.pop();
        if (client instanceof AsyncDataClient) {
          ((AsyncDataClient) client).close();
        } else if (client instanceof AsyncMetaClient) {
          ((AsyncMetaClient) client).close();
        }
      }
      nodeClientNumMap.put(clusterNode, 0);
      clientStack.notifyAll();
      NodeStatusManager.getINSTANCE().deactivate(node);
    }
  }

  @SuppressWarnings("squid:S1135")
  void onComplete(Node node) {
    NodeStatusManager.getINSTANCE().activate(node);
  }

  void recreateClient(Node node) {
    ClusterNode clusterNode = new ClusterNode(node);
    Deque<AsyncClient> clientStack =
        clientCaches.computeIfAbsent(clusterNode, n -> new ArrayDeque<>());
    synchronized (clientStack) {
      try {
        AsyncClient asyncClient = asyncClientFactory.getAsyncClient(node, this);
        clientStack.push(asyncClient);
      } catch (IOException e) {
        logger.error("Cannot create a new client for {}", node, e);
        nodeClientNumMap.computeIfPresent(clusterNode, (n, cnt) -> cnt - 1);
      }
      clientStack.notifyAll();
    }
  }

  @TestOnly
  public Map<ClusterNode, Integer> getNodeClientNumMap() {
    return nodeClientNumMap;
  }
}
