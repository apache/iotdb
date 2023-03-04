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

import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TProtocolFactory;

import java.util.concurrent.atomic.AtomicInteger;

public abstract class BaseFactory<K, T> implements KeyedPooledObjectFactory<K, T> {

  protected TAsyncClientManager[] managers;
  protected TProtocolFactory protocolFactory;
  protected AtomicInteger clientCnt = new AtomicInteger();
  protected ClientCategory category;
  protected IClientManager clientPoolManager;

  protected BaseFactory(TProtocolFactory protocolFactory, ClientCategory category) {
    this.protocolFactory = protocolFactory;
    this.category = category;
  }

  protected BaseFactory(
      TProtocolFactory protocolFactory, ClientCategory category, IClientManager clientManager) {
    this.protocolFactory = protocolFactory;
    this.category = category;
    this.clientPoolManager = clientManager;
  }

  @Override
  public void activateObject(K node, PooledObject<T> pooledObject) throws Exception {}

  @Override
  public void passivateObject(K node, PooledObject<T> pooledObject) throws Exception {}
}
