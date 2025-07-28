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

package org.apache.iotdb.commons.client.factory;

import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.exception.CreateTAsyncClientManagerException;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;

import org.apache.thrift.async.TAsyncClientManager;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public abstract class AsyncThriftClientFactory<K, V> extends ThriftClientFactory<K, V> {

  protected final TAsyncClientManager[] tManagers;
  protected final AtomicInteger clientCnt = new AtomicInteger();
  private static final String THRIFT_THREAD_NAME = "TAsyncClientManager#SelectorThread";

  protected AsyncThriftClientFactory(
      ClientManager<K, V> clientManager,
      ThriftClientProperty thriftClientProperty,
      String threadName) {
    super(clientManager, thriftClientProperty);
    try {
      tManagers = new TAsyncClientManager[thriftClientProperty.getSelectorNumOfAsyncClientPool()];
      for (int i = 0; i < tManagers.length; i++) {
        tManagers[i] = new TAsyncClientManager();
      }
    } catch (IOException e) {
      throw new CreateTAsyncClientManagerException(
          String.format("Cannot create Async thrift client factory %s", threadName), e);
    }
    Thread.getAllStackTraces().keySet().stream()
        .filter(thread -> thread.getName().contains(THRIFT_THREAD_NAME))
        .collect(Collectors.toList())
        .forEach(thread -> thread.setName(threadName + "-selector-" + thread.getId()));
  }

  public void close() {
    for (TAsyncClientManager tManager : tManagers) {
      tManager.stop();
    }
  }
}
