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

import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.IClientPoolFactory;
import org.apache.iotdb.commons.client.property.ClientPoolProperty;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.apache.commons.pool2.KeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;

public class DataNodeClientPoolFactory {

  private static final IoTDBConfig conf = IoTDBDescriptor.getInstance().getConfig();

  private DataNodeClientPoolFactory() {}

  public static class ConfigNodeClientPoolFactory
      implements IClientPoolFactory<ConfigRegionId, ConfigNodeClient> {

    @Override
    public KeyedObjectPool<ConfigRegionId, ConfigNodeClient> createClientPool(
        ClientManager<ConfigRegionId, ConfigNodeClient> manager) {
      return new GenericKeyedObjectPool<>(
          new ConfigNodeClient.Factory(
              manager,
              new ThriftClientProperty.Builder()
                  .setConnectionTimeoutMs(conf.getConnectionTimeoutInMS())
                  .setRpcThriftCompressionEnabled(conf.isRpcThriftCompressionEnable())
                  .build()),
          new ClientPoolProperty.Builder<ConfigNodeClient>()
              .setCoreClientNumForEachNode(conf.getCoreClientNumForEachNode())
              .setMaxClientNumForEachNode(conf.getMaxClientNumForEachNode())
              .build()
              .getConfig());
    }
  }

  public static class ClusterDeletionConfigNodeClientPoolFactory
      implements IClientPoolFactory<ConfigRegionId, ConfigNodeClient> {

    @Override
    public KeyedObjectPool<ConfigRegionId, ConfigNodeClient> createClientPool(
        ClientManager<ConfigRegionId, ConfigNodeClient> manager) {
      return new GenericKeyedObjectPool<>(
          new ConfigNodeClient.Factory(
              manager,
              new ThriftClientProperty.Builder()
                  .setConnectionTimeoutMs(conf.getConnectionTimeoutInMS() * 10)
                  .setRpcThriftCompressionEnabled(conf.isRpcThriftCompressionEnable())
                  .setSelectorNumOfAsyncClientManager(
                      conf.getSelectorNumOfClientManager() / 10 > 0
                          ? conf.getSelectorNumOfClientManager() / 10
                          : 1)
                  .build()),
          new ClientPoolProperty.Builder<ConfigNodeClient>()
              .setCoreClientNumForEachNode(conf.getCoreClientNumForEachNode())
              .setMaxClientNumForEachNode(conf.getMaxClientNumForEachNode())
              .build()
              .getConfig());
    }
  }
}
