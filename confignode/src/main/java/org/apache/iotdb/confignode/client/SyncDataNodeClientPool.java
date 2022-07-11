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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidateCacheReq;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidatePermissionCacheReq;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Synchronously send RPC requests to DataNodes. See mpp.thrift for more details. */
public class SyncDataNodeClientPool {

  private static final Logger LOGGER = LoggerFactory.getLogger(SyncDataNodeClientPool.class);

  private final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clientManager;

  private SyncDataNodeClientPool() {
    clientManager =
        new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
            .createClientManager(
                new ConfigNodeClientPoolFactory.SyncDataNodeInternalServiceClientPoolFactory());
  }

  public TSStatus invalidatePartitionCache(
      TEndPoint endPoint, TInvalidateCacheReq invalidateCacheReq) {
    TSStatus status;
    try (SyncDataNodeInternalServiceClient client = clientManager.borrowClient(endPoint)) {
      status = client.invalidatePartitionCache(invalidateCacheReq);
      LOGGER.info("Invalid Schema Cache {} successfully", invalidateCacheReq);
    } catch (IOException e) {
      LOGGER.error("Can't connect to DataNode {}", endPoint, e);
      status = new TSStatus(TSStatusCode.TIME_OUT.getStatusCode());
    } catch (TException e) {
      LOGGER.error("Invalid Schema Cache on DataNode {} failed", endPoint, e);
      status = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
    }
    return status;
  }

  public TSStatus invalidateSchemaCache(
      TEndPoint endPoint, TInvalidateCacheReq invalidateCacheReq) {
    TSStatus status;
    try (SyncDataNodeInternalServiceClient client = clientManager.borrowClient(endPoint)) {
      status = client.invalidateSchemaCache(invalidateCacheReq);
    } catch (IOException e) {
      LOGGER.error("Can't connect to DataNode {}", endPoint, e);
      status = new TSStatus(TSStatusCode.TIME_OUT.getStatusCode());
    } catch (TException e) {
      LOGGER.error("Invalid Schema Cache on DataNode {} failed", endPoint, e);
      status = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
    }
    return status;
  }

  public void deleteRegions(Set<TRegionReplicaSet> deletedRegionSet) {
    Map<TDataNodeLocation, List<TConsensusGroupId>> regionInfoMap = new HashMap<>();
    deletedRegionSet.forEach(
        (tRegionReplicaSet) -> {
          for (TDataNodeLocation dataNodeLocation : tRegionReplicaSet.getDataNodeLocations()) {
            regionInfoMap
                .computeIfAbsent(dataNodeLocation, k -> new ArrayList<>())
                .add(tRegionReplicaSet.getRegionId());
          }
        });
    LOGGER.info("Current regionInfoMap {} ", regionInfoMap);
    regionInfoMap.forEach(
        (dataNodeLocation, regionIds) ->
            deleteRegions(dataNodeLocation.getInternalEndPoint(), regionIds, deletedRegionSet));
  }

  private void deleteRegions(
      TEndPoint endPoint,
      List<TConsensusGroupId> regionIds,
      Set<TRegionReplicaSet> deletedRegionSet) {
    try (SyncDataNodeInternalServiceClient client = clientManager.borrowClient(endPoint)) {
      for (TConsensusGroupId regionId : regionIds) {
        LOGGER.debug("Delete region {} ", regionId);
        final TSStatus status = client.deleteRegion(regionId);
        if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          LOGGER.info("DELETE Region {} successfully", regionId);
          deletedRegionSet.removeIf(k -> k.getRegionId().equals(regionId));
        }
      }
    } catch (IOException e) {
      LOGGER.error("Can't connect to DataNode {}", endPoint, e);
    } catch (TException e) {
      LOGGER.error("Delete Region on DataNode {} failed", endPoint, e);
    }
  }

  public TSStatus invalidatePermissionCache(
      TEndPoint endPoint, TInvalidatePermissionCacheReq invalidatePermissionCacheReq) {
    TSStatus status;
    try (SyncDataNodeInternalServiceClient client = clientManager.borrowClient(endPoint)) {
      status = client.invalidatePermissionCache(invalidatePermissionCacheReq);
    } catch (IOException e) {
      LOGGER.error("Can't connect to DataNode {}", endPoint, e);
      status = new TSStatus(TSStatusCode.TIME_OUT.getStatusCode());
    } catch (TException e) {
      LOGGER.error("Invalid Permission Cache on DataNode {} failed", endPoint, e);
      status = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
    }
    return status;
  }

  // TODO: Is the ClientPool must be a singleton?
  private static class ClientPoolHolder {

    private static final SyncDataNodeClientPool INSTANCE = new SyncDataNodeClientPool();

    private ClientPoolHolder() {
      // Empty constructor
    }
  }

  public static SyncDataNodeClientPool getInstance() {
    return ClientPoolHolder.INSTANCE;
  }
}
