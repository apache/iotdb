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

import org.apache.iotdb.commons.client.exception.BorrowNullClientManagerException;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.utils.TestOnly;

import org.apache.commons.pool2.KeyedObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

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
  public V borrowClient(K node) throws ClientManagerException {
    if (node == null) {
      throw new BorrowNullClientManagerException();
    }
    try {
      return pool.borrowObject(node);
    } catch (Exception e) {
      throw new ClientManagerException(e);
    }
  }

  /**
   * return a client V for node K to the ClientManager.
   *
   * <p>Note: We do not define this interface in IClientManager to make you aware that the return of
   * a client is automatic whenever a particular client is used.
   */
  public void returnClient(K node, V client) {
    Optional.ofNullable(node)
        .ifPresent(
            x -> {
              try {
                pool.returnObject(node, client);
              } catch (Exception e) {
                logger.warn(
                    String.format("Return client %s for node %s to pool failed.", client, node), e);
              }
            });
  }

  @Override
  public void clear(K node) {
    Optional.ofNullable(node)
        .ifPresent(
            x -> {
              try {
                pool.clear(node);
              } catch (Exception e) {
                logger.warn(String.format("Clear all client in pool for node %s failed.", node), e);
              }
            });
  }

  @Override
  public void close() {
    pool.close();
  }
}
