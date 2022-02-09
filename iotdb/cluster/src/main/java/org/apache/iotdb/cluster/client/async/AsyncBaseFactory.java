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

import org.apache.iotdb.cluster.client.BaseFactory;
import org.apache.iotdb.cluster.client.ClientCategory;
import org.apache.iotdb.cluster.client.IClientManager;
import org.apache.iotdb.cluster.config.ClusterDescriptor;
import org.apache.iotdb.cluster.rpc.thrift.RaftService;

import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TProtocolFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public abstract class AsyncBaseFactory<K, T extends RaftService.AsyncClient>
    extends BaseFactory<K, T> {

  private static final Logger logger = LoggerFactory.getLogger(AsyncBaseFactory.class);

  protected AsyncBaseFactory(TProtocolFactory protocolFactory, ClientCategory category) {
    super(protocolFactory, category);
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

  protected AsyncBaseFactory(
      TProtocolFactory protocolFactory, ClientCategory category, IClientManager clientManager) {
    super(protocolFactory, category, clientManager);
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
