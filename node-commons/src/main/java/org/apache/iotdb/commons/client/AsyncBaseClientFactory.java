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

import org.apache.thrift.async.TAsyncClientManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public abstract class AsyncBaseClientFactory<K, V> extends BaseClientFactory<K, V> {

  private static final Logger logger = LoggerFactory.getLogger(AsyncBaseClientFactory.class);
  protected TAsyncClientManager[] tManagers;
  protected AtomicInteger clientCnt = new AtomicInteger();
  private static final String THRIFT_THREAD_NAME = "TAsyncClientManager#SelectorThread";

  protected AsyncBaseClientFactory(
      ClientManager<K, V> clientManager,
      ClientFactoryProperty clientFactoryProperty,
      String threadName) {
    super(clientManager, clientFactoryProperty);
    synchronized (this) {
      tManagers = new TAsyncClientManager[clientFactoryProperty.getSelectorNumOfAsyncClientPool()];
      for (int i = 0; i < tManagers.length; i++) {
        try {
          tManagers[i] = new TAsyncClientManager();
        } catch (IOException e) {
          logger.error("Cannot create Async client factory", e);
        }
      }
      Thread.getAllStackTraces().keySet().stream()
          .filter(thread -> thread.getName().contains(THRIFT_THREAD_NAME))
          .collect(Collectors.toList())
          .forEach(thread -> thread.setName(threadName + "-selector" + "-" + thread.getId()));
    }
  }

  protected void stopTManagers() {
    for (TAsyncClientManager tManager : tManagers) {
      tManager.stop();
    }
  }
}
