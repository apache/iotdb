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

package org.apache.iotdb.confignode.procedure.env;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.db.client.DataNodeClientPoolFactory;
import org.apache.iotdb.mpp.rpc.thrift.InternalService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class ConfigNodeProcedureEnv {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigNodeProcedureEnv.class);

  private final ConfigManager configManager;

  public ConfigNodeProcedureEnv(ConfigManager configManager) {
    this.configManager = configManager;
  }

  // TODO: reuse the same ClientPool with other module
  private static final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient>
      INTERNAL_SERVICE_CLIENT_MANAGER =
          new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
              .createClientManager(
                  new DataNodeClientPoolFactory.SyncDataNodeInternalServiceClientPoolFactory());

  public ConfigManager getConfigManager() {
    return configManager;
  }

  public InternalService.Client getDataNodeClient(TRegionReplicaSet dataRegionReplicaSet)
      throws IOException {
    List<TDataNodeLocation> dataNodeLocations = dataRegionReplicaSet.getDataNodeLocations();
    int retry = dataNodeLocations.size() - 1;
    for (TDataNodeLocation dataNodeLocation : dataNodeLocations) {
      try {
        return INTERNAL_SERVICE_CLIENT_MANAGER.borrowClient(dataNodeLocation.getInternalEndPoint());
      } catch (IOException e) {
        if (retry-- > 0) {
          LOG.warn(
              "Connect dataRegion-{} at dataNode-{} failed, trying next replica..",
              dataRegionReplicaSet.getRegionId(),
              dataNodeLocation);
        } else {
          LOG.warn("Connect dataRegion{} failed", dataRegionReplicaSet.getRegionId());
          throw e;
        }
      }
    }
    return null;
  }

  public InternalService.Client getDataNodeClient(TDataNodeLocation dataNodeLocation)
      throws IOException {
    return INTERNAL_SERVICE_CLIENT_MANAGER.borrowClient(dataNodeLocation.getInternalEndPoint());
  }
}
