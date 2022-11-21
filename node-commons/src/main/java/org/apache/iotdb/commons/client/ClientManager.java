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

package org.apache.iotdb.commons.client;

import org.apache.iotdb.commons.utils.TestOnly;

import org.apache.commons.pool2.KeyedObjectPool;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ClientManager<K, V> implements IClientManager<K, V> {

  private static final Logger logger = LoggerFactory.getLogger(ClientManager.class);

  private final KeyedObjectPool<K, V> pool;

  ClientManager(IClientPoolFactory<K, V> factory) {
    pool = factory.createClientPool(this);
  }

  @TestOnly
  public KeyedObjectPool<K, V> getPool() {
    return pool;
  }

  @Override
  public V borrowClient(K node) throws IOException {
    V client;
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
      String errorMessage = String.format("Borrow client from pool for node %s failed.", node);
      throw new IOException(errorMessage, e);
    }
    return client;
  }

  @Override
  public V purelyBorrowClient(K node) {
    V client = null;
    try {
      client = pool.borrowObject(node);
    } catch (Exception ignored) {
      // Just ignore
    }
    return client;
  }

  // return a V client of the K node to the Manager
  public void returnClient(K node, V client) {
    if (client != null && node != null) {
      try {
        pool.returnObject(node, client);
      } catch (Exception e) {
        logger.error(
            String.format("Return client %s for node %s to pool failed.", client, node), e);
      }
    }
  }

  @Override
  public void clear(K node) {
    if (node != null) {
      try {
        pool.clear(node);
      } catch (Exception e) {
        logger.error(String.format("Clear all client in pool for node %s failed.", node), e);
      }
    }
  }

  @Override
  public void close() {
    try {
      pool.close();
    } catch (Exception e) {
      logger.error("close client pool failed", e);
    }
  }
}
