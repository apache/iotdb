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

package org.apache.iotdb.db.client;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.ClientManagerProperty;
import org.apache.iotdb.commons.client.IClientPoolFactory;
import org.apache.iotdb.commons.client.async.AsyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.client.async.AsyncDataNodeDataBlockServiceClient;
import org.apache.iotdb.commons.client.async.AsyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.client.sync.SyncDataNodeDataBlockServiceClient;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.apache.commons.pool2.KeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;

public class ClientPoolFactory {

  private static final IoTDBConfig conf = IoTDBDescriptor.getInstance().getConfig();

  private ClientPoolFactory() {}

  public static class SyncConfigNodeIServiceClientPoolFactory
      implements IClientPoolFactory<TEndPoint, SyncConfigNodeIServiceClient> {
    @Override
    public KeyedObjectPool<TEndPoint, SyncConfigNodeIServiceClient> createClientPool(
        ClientManager<TEndPoint, SyncConfigNodeIServiceClient> manager) {
      ClientManagerProperty<SyncConfigNodeIServiceClient> property =
          new ClientManagerProperty.Builder<SyncConfigNodeIServiceClient>()
              .setConnectionTimeoutMs(conf.getConnectionTimeoutInMS())
              .setRpcThriftCompressionEnabled(conf.isRpcThriftCompressionEnable())
              .build();
      return new GenericKeyedObjectPool<>(
          new SyncConfigNodeIServiceClient.Factory(manager, property), property.getConfig());
    }
  }

  public static class AsyncConfigNodeIServiceClientPoolFactory
      implements IClientPoolFactory<TEndPoint, AsyncConfigNodeIServiceClient> {
    @Override
    public KeyedObjectPool<TEndPoint, AsyncConfigNodeIServiceClient> createClientPool(
        ClientManager<TEndPoint, AsyncConfigNodeIServiceClient> manager) {
      ClientManagerProperty<AsyncConfigNodeIServiceClient> property =
          new ClientManagerProperty.Builder<AsyncConfigNodeIServiceClient>()
              .setConnectionTimeoutMs(conf.getConnectionTimeoutInMS())
              .setRpcThriftCompressionEnabled(conf.isRpcThriftCompressionEnable())
              .setSelectorNumOfAsyncClientPool(conf.getSelectorNumOfClientPool())
              .build();
      return new GenericKeyedObjectPool<>(
          new AsyncConfigNodeIServiceClient.Factory(manager, property), property.getConfig());
    }
  }

  public static class SyncDataNodeInternalServiceClientPoolFactory
      implements IClientPoolFactory<TEndPoint, SyncDataNodeInternalServiceClient> {
    @Override
    public KeyedObjectPool<TEndPoint, SyncDataNodeInternalServiceClient> createClientPool(
        ClientManager<TEndPoint, SyncDataNodeInternalServiceClient> manager) {
      ClientManagerProperty<SyncDataNodeInternalServiceClient> property =
          new ClientManagerProperty.Builder<SyncDataNodeInternalServiceClient>()
              .setConnectionTimeoutMs(conf.getConnectionTimeoutInMS())
              .setRpcThriftCompressionEnabled(conf.isRpcThriftCompressionEnable())
              .build();
      return new GenericKeyedObjectPool<>(
          new SyncDataNodeInternalServiceClient.Factory(manager, property), property.getConfig());
    }
  }

  public static class AsyncDataNodeInternalServiceClientPoolFactory
      implements IClientPoolFactory<TEndPoint, AsyncDataNodeInternalServiceClient> {
    @Override
    public KeyedObjectPool<TEndPoint, AsyncDataNodeInternalServiceClient> createClientPool(
        ClientManager<TEndPoint, AsyncDataNodeInternalServiceClient> manager) {
      ClientManagerProperty<AsyncDataNodeInternalServiceClient> property =
          new ClientManagerProperty.Builder<AsyncDataNodeInternalServiceClient>()
              .setConnectionTimeoutMs(conf.getConnectionTimeoutInMS())
              .setRpcThriftCompressionEnabled(conf.isRpcThriftCompressionEnable())
              .setSelectorNumOfAsyncClientPool(conf.getSelectorNumOfClientPool())
              .build();
      return new GenericKeyedObjectPool<>(
          new AsyncDataNodeInternalServiceClient.Factory(manager, property), property.getConfig());
    }
  }

  public static class SyncDataNodeDataBlockServiceClientPoolFactory
      implements IClientPoolFactory<TEndPoint, SyncDataNodeDataBlockServiceClient> {
    @Override
    public KeyedObjectPool<TEndPoint, SyncDataNodeDataBlockServiceClient> createClientPool(
        ClientManager<TEndPoint, SyncDataNodeDataBlockServiceClient> manager) {
      ClientManagerProperty<SyncDataNodeDataBlockServiceClient> property =
          new ClientManagerProperty.Builder<SyncDataNodeDataBlockServiceClient>()
              .setConnectionTimeoutMs(conf.getConnectionTimeoutInMS())
              .setRpcThriftCompressionEnabled(conf.isRpcThriftCompressionEnable())
              .build();
      return new GenericKeyedObjectPool<>(
          new SyncDataNodeDataBlockServiceClient.Factory(manager, property), property.getConfig());
    }
  }

  public static class AsyncDataNodeDataBlockServiceClientPoolFactory
      implements IClientPoolFactory<TEndPoint, AsyncDataNodeDataBlockServiceClient> {
    @Override
    public KeyedObjectPool<TEndPoint, AsyncDataNodeDataBlockServiceClient> createClientPool(
        ClientManager<TEndPoint, AsyncDataNodeDataBlockServiceClient> manager) {
      ClientManagerProperty<AsyncDataNodeDataBlockServiceClient> property =
          new ClientManagerProperty.Builder<AsyncDataNodeDataBlockServiceClient>()
              .setConnectionTimeoutMs(conf.getConnectionTimeoutInMS())
              .setRpcThriftCompressionEnabled(conf.isRpcThriftCompressionEnable())
              .setSelectorNumOfAsyncClientPool(conf.getSelectorNumOfClientPool())
              .build();
      return new GenericKeyedObjectPool<>(
          new AsyncDataNodeDataBlockServiceClient.Factory(manager, property), property.getConfig());
    }
  }
}
