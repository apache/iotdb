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

package org.apache.iotdb.confignode.client;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientFactoryProperty;
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.ClientPoolProperty;
import org.apache.iotdb.commons.client.IClientPoolFactory;
import org.apache.iotdb.commons.client.async.AsyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;

import org.apache.commons.pool2.KeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;

public class ConfigNodeClientPoolFactory {

  private static final ConfigNodeConfig conf = ConfigNodeDescriptor.getInstance().getConf();

  private ConfigNodeClientPoolFactory() {}

  public static class SyncDataNodeInternalServiceClientPoolFactory
      implements IClientPoolFactory<TEndPoint, SyncDataNodeInternalServiceClient> {
    @Override
    public KeyedObjectPool<TEndPoint, SyncDataNodeInternalServiceClient> createClientPool(
        ClientManager<TEndPoint, SyncDataNodeInternalServiceClient> manager) {
      return new GenericKeyedObjectPool<>(
          new SyncDataNodeInternalServiceClient.Factory(
              manager,
              new ClientFactoryProperty.Builder()
                  .setConnectionTimeoutMs(conf.getConnectionTimeoutInMS())
                  .setRpcThriftCompressionEnabled(conf.isRpcThriftCompressionEnabled())
                  .setSelectorNumOfAsyncClientManager(conf.getSelectorNumOfClientManager())
                  .build()),
          new ClientPoolProperty.Builder<SyncDataNodeInternalServiceClient>().build().getConfig());
    }
  }

  public static class AsyncDataNodeInternalServiceClientPoolFactory
      implements IClientPoolFactory<TEndPoint, AsyncDataNodeInternalServiceClient> {
    @Override
    public KeyedObjectPool<TEndPoint, AsyncDataNodeInternalServiceClient> createClientPool(
        ClientManager<TEndPoint, AsyncDataNodeInternalServiceClient> manager) {
      return new GenericKeyedObjectPool<>(
          new AsyncDataNodeInternalServiceClient.Factory(
              manager,
              new ClientFactoryProperty.Builder()
                  .setConnectionTimeoutMs(conf.getConnectionTimeoutInMS())
                  .setRpcThriftCompressionEnabled(conf.isRpcThriftCompressionEnabled())
                  .setSelectorNumOfAsyncClientManager(conf.getSelectorNumOfClientManager())
                  .build()),
          new ClientPoolProperty.Builder<AsyncDataNodeInternalServiceClient>().build().getConfig());
    }
  }
}
