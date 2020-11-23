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

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;
import org.apache.iotdb.cluster.utils.ClusterNode;
import org.apache.thrift.async.TAsyncMethodCall;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncClientPool {

  private static final Logger logger = LoggerFactory.getLogger(AsyncClientPool.class);
  private static final long WAIT_CLIENT_TIMEOUT_MS = 5 * 1000L;
  private int maxConnectionForEachNode;
  private Map<ClusterNode, Deque<AsyncClient>> clientCaches = new ConcurrentHashMap<>();
  private Map<ClusterNode, Integer> nodeClientNumMap = new ConcurrentHashMap<>();
  private Map<ClusterNode, Integer> nodeErrorClientCountMap = new ConcurrentHashMap<>();
  private AsyncClientFactory asyncClientFactory;
  private ScheduledExecutorService cleanErrorClientExecutorService;
  // when set to true, if MAX_ERROR_COUNT errors occurs continuously when connecting to node, any
  // further requests to the node will be rejected for PROBE_NODE_STATUS_PERIOD_SECOND
  // heartbeats should not be blocked
  private boolean blockOnError;

  private static final int MAX_ERROR_COUNT = 3;
  private static final int PROBE_NODE_STATUS_PERIOD_SECOND = 60;

  public AsyncClientPool(AsyncClientFactory asyncClientFactory) {
    this(asyncClientFactory, true);
  }

  public AsyncClientPool(AsyncClientFactory asyncClientFactory, boolean blockOnError) {
    this.asyncClientFactory = asyncClientFactory;
    this.maxConnectionForEachNode =
        ClusterDescriptor.getInstance().getConfig().getMaxClientPerNodePerMember();
    this.blockOnError = blockOnError;
    if (blockOnError) {
      this.cleanErrorClientExecutorService = new ScheduledThreadPoolExecutor(1,
          new BasicThreadFactory.Builder().namingPattern("clean-error-client-%d").daemon(true)
              .build());
      this.cleanErrorClientExecutorService
          .scheduleAtFixedRate(this::cleanErrorClients, PROBE_NODE_STATUS_PERIOD_SECOND,
              PROBE_NODE_STATUS_PERIOD_SECOND, TimeUnit.SECONDS);
    }
  }

  /**
   * Get a client of the given node from the cache if one is available, or create a new one.
   *
   * @param node
   * @return
   * @throws IOException
   */
  public AsyncClient getClient(Node node) throws IOException {
    ClusterNode clusterNode = new ClusterNode(node);
    if (blockOnError && nodeErrorClientCountMap.getOrDefault(clusterNode, 0) > MAX_ERROR_COUNT) {
      throw new IOException(String.format("connect node failed, maybe the node is down, %s", node));
    }

    AsyncClient client;
    synchronized (this) {
      //As clientCaches is ConcurrentHashMap, computeIfAbsent is thread safety.
      Deque<AsyncClient> clientStack = clientCaches.computeIfAbsent(clusterNode,
          n -> new ArrayDeque<>());
      if (clientStack.isEmpty()) {
        int nodeClientNum = nodeClientNumMap.getOrDefault(clusterNode, 0);
        if (nodeClientNum >= maxConnectionForEachNode) {
          client = waitForClient(clientStack, clusterNode, nodeClientNum);
        } else {
          nodeClientNumMap.put(clusterNode, nodeClientNum + 1);
          client = asyncClientFactory.getAsyncClient(clusterNode, this);
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
   * @param node
   * @param nodeClientNum
   * @return
   * @throws IOException
   */
  @SuppressWarnings({"squid:S2273"}) // synchronized outside
  private AsyncClient waitForClient(Deque<AsyncClient> clientStack, ClusterNode node,
      int nodeClientNum)
      throws IOException {
    // wait for an available client
    long waitStart = System.currentTimeMillis();
    while (clientStack.isEmpty()) {
      try {
        this.wait(WAIT_CLIENT_TIMEOUT_MS);
        if (clientStack.isEmpty()
            && System.currentTimeMillis() - waitStart >= WAIT_CLIENT_TIMEOUT_MS) {
          logger.warn("Cannot get an available client after {}ms, create a new one, factory {} now is {}",
              WAIT_CLIENT_TIMEOUT_MS, asyncClientFactory, nodeClientNum);
          nodeClientNumMap.put(node, nodeClientNum + 1);
          return asyncClientFactory.getAsyncClient(node, this);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn("Interrupted when waiting for an available client of {}", node);
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
    synchronized (this) {
      //As clientCaches is ConcurrentHashMap, computeIfAbsent is thread safety.
      Deque<AsyncClient> clientStack = clientCaches
          .computeIfAbsent(clusterNode, n -> new ArrayDeque<>());
      clientStack.push(client);
      this.notifyAll();
    }
  }

  void onError(Node node) {
    ClusterNode clusterNode = new ClusterNode(node);
    // clean all cached clients when network fails
    synchronized (this) {
      Deque<AsyncClient> clientStack = clientCaches
          .computeIfAbsent(clusterNode, n -> new ArrayDeque<>());
      while (!clientStack.isEmpty()) {
        AsyncClient client = clientStack.pop();
        if (client instanceof AsyncDataClient) {
          ((AsyncDataClient) client).close();
        } else if (client instanceof AsyncMetaClient) {
          ((AsyncMetaClient) client).close();
        }
      }
      clientStack.clear();
      nodeClientNumMap.put(clusterNode, 0);
      this.notifyAll();
    }
    if (!blockOnError) {
      return;
    }
    synchronized (this) {
      if (nodeErrorClientCountMap.containsKey(clusterNode)) {
        nodeErrorClientCountMap.put(clusterNode, nodeErrorClientCountMap.get(clusterNode) + 1);
      } else {
        nodeErrorClientCountMap.put(clusterNode, 1);
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("the node={}, connect error times={}", clusterNode,
          nodeErrorClientCountMap.get(clusterNode));
    }
  }

  @SuppressWarnings("squid:S1135")
  void onComplete(Node node) {
    ClusterNode clusterNode = new ClusterNode(node);
    // TODO: if the heartbeat client pool completes, also unblock another pool
    nodeErrorClientCountMap.remove(clusterNode);
  }

  void cleanErrorClients() {
    synchronized (this) {
      nodeErrorClientCountMap.clear();
      logger.debug("clean all error clients");
    }
  }

  void recreateClient(Node node) {
    ClusterNode clusterNode = new ClusterNode(node);
    synchronized (this) {
      Deque<AsyncClient> clientStack = clientCaches
          .computeIfAbsent(clusterNode, n -> new ArrayDeque<>());
      try {
        AsyncClient asyncClient = asyncClientFactory.getAsyncClient(node, this);
        clientStack.push(asyncClient);
      } catch (IOException e) {
        logger.error("Cannot create a new client for {}", node, e);
        nodeClientNumMap.computeIfPresent(clusterNode, (n, cnt) -> cnt - 1);
      }
      this.notifyAll();
    }
  }
}
