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

package org.apache.iotdb.db.protocol.client;

import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.ClientManagerMetrics;
import org.apache.iotdb.commons.client.IClientPoolFactory;
import org.apache.iotdb.commons.client.property.ClientPoolProperty;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.client.an.AINodeClient;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;

public class DataNodeClientPoolFactory {

  private static final IoTDBConfig CONF = IoTDBDescriptor.getInstance().getConfig();

  private DataNodeClientPoolFactory() {
    // Empty constructor
  }

  public static class ConfigNodeClientPoolFactory
      implements IClientPoolFactory<ConfigRegionId, ConfigNodeClient> {

    @Override
    public GenericKeyedObjectPool<ConfigRegionId, ConfigNodeClient> createClientPool(
        ClientManager<ConfigRegionId, ConfigNodeClient> manager) {
      GenericKeyedObjectPool<ConfigRegionId, ConfigNodeClient> clientPool =
          new GenericKeyedObjectPool<>(
              new ConfigNodeClient.Factory(
                  manager,
                  new ThriftClientProperty.Builder()
                      .setConnectionTimeoutMs(CONF.getConnectionTimeoutInMS())
                      .setRpcThriftCompressionEnabled(CONF.isRpcThriftCompressionEnable())
                      .build()),
              new ClientPoolProperty.Builder<ConfigNodeClient>()
                  .setMaxClientNumForEachNode(CONF.getMaxClientNumForEachNode())
                  .build()
                  .getConfig());
      ClientManagerMetrics.getInstance()
          .registerClientManager(this.getClass().getSimpleName(), clientPool);
      return clientPool;
    }
  }

  public static class ClusterDeletionConfigNodeClientPoolFactory
      implements IClientPoolFactory<ConfigRegionId, ConfigNodeClient> {

    @Override
    public GenericKeyedObjectPool<ConfigRegionId, ConfigNodeClient> createClientPool(
        ClientManager<ConfigRegionId, ConfigNodeClient> manager) {
      GenericKeyedObjectPool<ConfigRegionId, ConfigNodeClient> clientPool =
          new GenericKeyedObjectPool<>(
              new ConfigNodeClient.Factory(
                  manager,
                  new ThriftClientProperty.Builder()
                      .setConnectionTimeoutMs(CONF.getConnectionTimeoutInMS() * 10)
                      .setRpcThriftCompressionEnabled(CONF.isRpcThriftCompressionEnable())
                      .setSelectorNumOfAsyncClientManager(
                          CONF.getSelectorNumOfClientManager() / 10 > 0
                              ? CONF.getSelectorNumOfClientManager() / 10
                              : 1)
                      .build()),
              new ClientPoolProperty.Builder<ConfigNodeClient>()
                  .setMaxClientNumForEachNode(CONF.getMaxClientNumForEachNode())
                  .build()
                  .getConfig());
      ClientManagerMetrics.getInstance()
          .registerClientManager(this.getClass().getSimpleName(), clientPool);
      return clientPool;
    }
  }

  public static class AINodeClientPoolFactory implements IClientPoolFactory<Integer, AINodeClient> {

    @Override
    public GenericKeyedObjectPool<Integer, AINodeClient> createClientPool(
        ClientManager<Integer, AINodeClient> manager) {
      GenericKeyedObjectPool<Integer, AINodeClient> clientPool =
          new GenericKeyedObjectPool<>(
              new AINodeClient.Factory(
                  manager,
                  new ThriftClientProperty.Builder()
                      .setConnectionTimeoutMs(CONF.getConnectionTimeoutInMS())
                      .setRpcThriftCompressionEnabled(CONF.isRpcThriftCompressionEnable())
                      .build()),
              new ClientPoolProperty.Builder<AINodeClient>()
                  .setMaxClientNumForEachNode(CONF.getMaxClientNumForEachNode())
                  .build()
                  .getConfig());
      ClientManagerMetrics.getInstance()
          .registerClientManager(this.getClass().getSimpleName(), clientPool);
      return clientPool;
    }
  }
}
