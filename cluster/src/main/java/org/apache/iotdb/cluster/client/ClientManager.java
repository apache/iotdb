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

import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.rpc.thrift.RaftService;

import com.google.common.collect.Maps;
import org.apache.commons.pool2.KeyedObjectPool;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * One should borrow the reusable client from this manager and return the client after use. The
 * underlying client pool is powered by Apache Commons Pool. The class provided 3 default pool group
 * according to current usage: RequestForwardClient, DataGroupClients, MetaGroupClients.
 *
 * <p>TODO: We can refine the client structure by reorg the interfaces defined in cluster-thrift.
 */
public class ClientManager implements IClientManager {

  private static final Logger logger = LoggerFactory.getLogger(ClientManager.class);

  private Map<ClientCategory, KeyedObjectPool<Node, RaftService.AsyncClient>> asyncClientPoolMap;
  private Map<ClientCategory, KeyedObjectPool<Node, RaftService.Client>> syncClientPoolMap;
  private final ClientPoolFactory clientPoolFactory;

  /**
   * {@link ClientManager.Type#RequestForwardClient} represents the clients used to forward external
   * client requests to proper node to handle such as query, insert request.
   *
   * <p>{@link ClientManager.Type#DataGroupClient} represents the clients used to appendEntry,
   * appendEntries, sendHeartbeat, etc for data raft group.
   *
   * <p>{@link ClientManager.Type#MetaGroupClient} represents the clients used to appendEntry,
   * appendEntries, sendHeartbeat, etc for meta raft group. *
   */
  public enum Type {
    RequestForwardClient,
    DataGroupClient,
    MetaGroupClient
  }

  public ClientManager(boolean isAsyncMode, Type type) {
    clientPoolFactory = new ClientPoolFactory();
    clientPoolFactory.setClientManager(this);
    if (isAsyncMode) {
      asyncClientPoolMap = Maps.newHashMap();
      constructAsyncClientMap(type);
    } else {
      syncClientPoolMap = Maps.newHashMap();
      constructSyncClientMap(type);
    }
  }

  private void constructAsyncClientMap(Type type) {
    switch (type) {
      case RequestForwardClient:
        asyncClientPoolMap.put(
            ClientCategory.DATA, clientPoolFactory.createAsyncDataPool(ClientCategory.DATA));
        break;
      case MetaGroupClient:
        asyncClientPoolMap.put(
            ClientCategory.META, clientPoolFactory.createAsyncMetaPool(ClientCategory.META));
        asyncClientPoolMap.put(
            ClientCategory.META_HEARTBEAT,
            clientPoolFactory.createAsyncMetaPool(ClientCategory.META_HEARTBEAT));
        break;
      case DataGroupClient:
        asyncClientPoolMap.put(
            ClientCategory.DATA, clientPoolFactory.createAsyncDataPool(ClientCategory.DATA));
        asyncClientPoolMap.put(
            ClientCategory.DATA_HEARTBEAT,
            clientPoolFactory.createAsyncDataPool(ClientCategory.DATA_HEARTBEAT));
        asyncClientPoolMap.put(
            ClientCategory.DATA_ASYNC_APPEND_CLIENT,
            clientPoolFactory.createSingleManagerAsyncDataPool());
        break;
      default:
        logger.warn("unsupported ClientManager type: {}", type);
        break;
    }
  }

  private void constructSyncClientMap(Type type) {
    switch (type) {
      case RequestForwardClient:
        syncClientPoolMap.put(
            ClientCategory.DATA, clientPoolFactory.createSyncDataPool(ClientCategory.DATA));
        break;
      case MetaGroupClient:
        syncClientPoolMap.put(
            ClientCategory.META, clientPoolFactory.createSyncMetaPool(ClientCategory.META));
        syncClientPoolMap.put(
            ClientCategory.META_HEARTBEAT,
            clientPoolFactory.createSyncMetaPool(ClientCategory.META_HEARTBEAT));
        break;
      case DataGroupClient:
        syncClientPoolMap.put(
            ClientCategory.DATA, clientPoolFactory.createSyncDataPool(ClientCategory.DATA));
        syncClientPoolMap.put(
            ClientCategory.DATA_HEARTBEAT,
            clientPoolFactory.createSyncDataPool(ClientCategory.DATA_HEARTBEAT));
        break;
      default:
        logger.warn("unsupported ClientManager type: {}", type);
        break;
    }
  }

  /**
   * It's safe to convert: 1. RaftService.AsyncClient to TSDataService.AsyncClient when category is
   * DATA or DATA_HEARTBEAT; 2. RaftService.AsyncClient to TSMetaService.AsyncClient when category
   * is META or META_HEARTBEAT.
   *
   * @return RaftService.AsyncClient
   */
  @Override
  public RaftService.AsyncClient borrowAsyncClient(Node node, ClientCategory category)
      throws IOException {
    KeyedObjectPool<Node, RaftService.AsyncClient> pool;
    RaftService.AsyncClient client = null;
    if (asyncClientPoolMap != null && (pool = asyncClientPoolMap.get(category)) != null) {
      try {
        client = pool.borrowObject(node);
      } catch (IOException e) {
        // external needs the IOException to check connection
        throw e;
      } catch (Exception e) {
        // external doesn't care of other exceptions
        logger.error("BorrowAsyncClient fail.", e);
      }
    } else {
      logger.warn(
          "BorrowSyncClient invoke on unsupported mode or category: Node:{}, ClientCategory:{}, "
              + "isSyncMode:{}",
          node,
          clientPoolFactory,
          syncClientPoolMap != null);
    }
    return client;
  }

  /**
   * It's safe to convert: 1. RaftService.Client to TSDataService.Client when category is DATA or
   * DATA_HEARTBEAT; 2. RaftService.Client to TSMetaService.Client when category is META or
   * META_HEARTBEAT.
   *
   * @return RaftService.Client
   */
  @Override
  public RaftService.Client borrowSyncClient(Node node, ClientCategory category)
      throws IOException {
    KeyedObjectPool<Node, RaftService.Client> pool;
    RaftService.Client client = null;
    if (syncClientPoolMap != null && (pool = syncClientPoolMap.get(category)) != null) {
      try {
        client = pool.borrowObject(node);
      } catch (TTransportException e) {
        // external needs to check transport related exception
        throw new IOException(e);
      } catch (IOException e) {
        // external needs the IOException to check connection
        throw e;
      } catch (Exception e) {
        // external doesn't care of other exceptions
        logger.error("BorrowSyncClient fail.", e);
      }
    } else {
      logger.warn(
          "BorrowSyncClient invoke on unsupported mode or category: Node:{}, ClientCategory:{}, "
              + "isSyncMode:{}",
          node,
          clientPoolFactory,
          syncClientPoolMap != null);
    }
    return client;
  }

  @Override
  public void returnAsyncClient(
      RaftService.AsyncClient client, Node node, ClientCategory category) {
    if (client != null && node != null) {
      try {
        asyncClientPoolMap.get(category).returnObject(node, client);
      } catch (Exception e) {
        logger.error("AsyncClient return error: {}", client, e);
      }
    }
  }

  @Override
  public void returnSyncClient(RaftService.Client client, Node node, ClientCategory category) {
    if (client != null && node != null) {
      try {
        syncClientPoolMap.get(category).returnObject(node, client);
      } catch (Exception e) {
        logger.error("SyncClient return error: {}", client, e);
      }
    }
  }
}
