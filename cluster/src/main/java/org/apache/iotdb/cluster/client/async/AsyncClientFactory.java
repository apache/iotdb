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
import org.apache.iotdb.cluster.rpc.thrift.RaftService;

import org.apache.thrift.async.TAsyncClientManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AsyncClientFactory {

  private static final Logger logger = LoggerFactory.getLogger(AsyncClientFactory.class);
  static TAsyncClientManager[] managers;
  org.apache.thrift.protocol.TProtocolFactory protocolFactory;
  AtomicInteger clientCnt = new AtomicInteger();

  static {
    if (ClusterDescriptor.getInstance().getConfig().isUseAsyncServer()) {
      managers =
          new TAsyncClientManager
              [ClusterDescriptor.getInstance().getConfig().getSelectorNumOfClientPool()];
      for (int i = 0; i < managers.length; i++) {
        try {
          managers[i] = new TAsyncClientManager();
        } catch (IOException e) {
          logger.error("Cannot create data heartbeat client manager for factory", e);
        }
      }
    }
  }

  /**
   * Get a client which will connect the given node and be cached in the given pool.
   *
   * @param node the cluster node the client will connect.
   * @param pool the pool that will cache the client for reusing.
   * @return
   * @throws IOException
   */
  protected abstract RaftService.AsyncClient getAsyncClient(Node node, AsyncClientPool pool)
      throws IOException;
}
