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

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public interface IClientManager<K, V> {

  /**
   * get a client V for node K from the IClientManager.
   *
   * @param node target node
   * @return client
   * @throws BorrowNullClientManagerException if node is null
   * @throws ClientManagerException for other exceptions
   */
  V borrowClient(K node) throws ClientManagerException;

  /**
   * clear all clients for node K.
   *
   * @param node target node
   */
  void clear(K node);

  /** close IClientManager, which means closing all clients for all nodes. */
  void close();

  class Factory<K, V> {

    public IClientManager<K, V> createClientManager(IClientPoolFactory<K, V> clientPoolFactory) {
      return new ClientManager<>(clientPoolFactory);
    }
  }
}
