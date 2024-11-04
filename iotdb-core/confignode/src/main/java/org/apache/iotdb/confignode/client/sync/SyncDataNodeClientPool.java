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

package org.apache.iotdb.confignode.client.sync;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.exception.UncheckedStartupException;
import org.apache.iotdb.mpp.rpc.thrift.TCleanDataNodeCacheReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreateDataRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreatePeerReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreateSchemaRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidateCacheReq;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidatePermissionCacheReq;
import org.apache.iotdb.mpp.rpc.thrift.TMaintainPeerReq;
import org.apache.iotdb.mpp.rpc.thrift.TRegionLeaderChangeReq;
import org.apache.iotdb.mpp.rpc.thrift.TRegionLeaderChangeResp;
import org.apache.iotdb.mpp.rpc.thrift.TResetPeerListReq;
import org.apache.iotdb.mpp.rpc.thrift.TUpdateTableReq;
import org.apache.iotdb.mpp.rpc.thrift.TUpdateTemplateReq;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.collect.ImmutableMap;
import org.apache.ratis.util.function.CheckedBiFunction;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** Synchronously send RPC requests to DataNodes. See queryengine.thrift for more details. */
public class SyncDataNodeClientPool {

  private static final Logger LOGGER = LoggerFactory.getLogger(SyncDataNodeClientPool.class);

  private static final int DEFAULT_RETRY_NUM = 6;

  private final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clientManager;

  protected ImmutableMap<
          CnToDnSyncRequestType,
          CheckedBiFunction<Object, SyncDataNodeInternalServiceClient, Object, Exception>>
      actionMap;

  private SyncDataNodeClientPool() {
    clientManager =
        new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
            .createClientManager(
                new ClientPoolFactory.SyncDataNodeInternalServiceClientPoolFactory());
    buildActionMap();
    checkActionMapCompleteness();
  }

  private void buildActionMap() {
    ImmutableMap.Builder<
            CnToDnSyncRequestType,
            CheckedBiFunction<Object, SyncDataNodeInternalServiceClient, Object, Exception>>
        actionMapBuilder = ImmutableMap.builder();
    actionMapBuilder.put(
        CnToDnSyncRequestType.INVALIDATE_PARTITION_CACHE,
        (req, client) -> client.invalidatePartitionCache((TInvalidateCacheReq) req));
    actionMapBuilder.put(
        CnToDnSyncRequestType.INVALIDATE_SCHEMA_CACHE,
        (req, client) -> client.invalidateSchemaCache((TInvalidateCacheReq) req));
    actionMapBuilder.put(
        CnToDnSyncRequestType.CREATE_SCHEMA_REGION,
        (req, client) -> client.createSchemaRegion((TCreateSchemaRegionReq) req));
    actionMapBuilder.put(
        CnToDnSyncRequestType.CREATE_DATA_REGION,
        (req, client) -> client.createDataRegion((TCreateDataRegionReq) req));
    actionMapBuilder.put(
        CnToDnSyncRequestType.DELETE_REGION,
        (req, client) -> client.deleteRegion((TConsensusGroupId) req));
    actionMapBuilder.put(
        CnToDnSyncRequestType.INVALIDATE_PERMISSION_CACHE,
        (req, client) -> client.invalidatePermissionCache((TInvalidatePermissionCacheReq) req));
    actionMapBuilder.put(
        CnToDnSyncRequestType.CLEAN_DATA_NODE_CACHE,
        (req, client) -> client.cleanDataNodeCache((TCleanDataNodeCacheReq) req));
    actionMapBuilder.put(
        CnToDnSyncRequestType.STOP_AND_CLEAR_DATA_NODE,
        (req, client) -> client.stopAndClearDataNode());
    actionMapBuilder.put(
        CnToDnSyncRequestType.SET_SYSTEM_STATUS,
        (req, client) -> client.setSystemStatus((String) req));
    actionMapBuilder.put(
        CnToDnSyncRequestType.KILL_QUERY_INSTANCE,
        (req, client) -> client.killQueryInstance((String) req));
    actionMapBuilder.put(
        CnToDnSyncRequestType.UPDATE_TEMPLATE,
        (req, client) -> client.updateTemplate((TUpdateTemplateReq) req));
    actionMapBuilder.put(
        CnToDnSyncRequestType.UPDATE_TABLE,
        (req, client) -> client.updateTable((TUpdateTableReq) req));
    actionMapBuilder.put(
        CnToDnSyncRequestType.CREATE_NEW_REGION_PEER,
        (req, client) -> client.createNewRegionPeer((TCreatePeerReq) req));
    actionMapBuilder.put(
        CnToDnSyncRequestType.ADD_REGION_PEER,
        (req, client) -> client.addRegionPeer((TMaintainPeerReq) req));
    actionMapBuilder.put(
        CnToDnSyncRequestType.REMOVE_REGION_PEER,
        (req, client) -> client.removeRegionPeer((TMaintainPeerReq) req));
    actionMapBuilder.put(
        CnToDnSyncRequestType.DELETE_OLD_REGION_PEER,
        (req, client) -> client.deleteOldRegionPeer((TMaintainPeerReq) req));
    actionMapBuilder.put(
        CnToDnSyncRequestType.RESET_PEER_LIST,
        (req, client) -> client.resetPeerList((TResetPeerListReq) req));
    actionMapBuilder.put(
        CnToDnSyncRequestType.SHOW_CONFIGURATION, (req, client) -> client.showConfiguration());
    actionMap = actionMapBuilder.build();
  }

  private void checkActionMapCompleteness() {
    List<CnToDnSyncRequestType> lackList =
        Arrays.stream(CnToDnSyncRequestType.values())
            .filter(type -> !actionMap.containsKey(type))
            .collect(Collectors.toList());
    if (!lackList.isEmpty()) {
      throw new UncheckedStartupException(
          String.format("These request types should be added to actionMap: %s", lackList));
    }
  }

  public Object sendSyncRequestToDataNodeWithRetry(
      TEndPoint endPoint, Object req, CnToDnSyncRequestType requestType) {
    Throwable lastException = new TException();
    for (int retry = 0; retry < DEFAULT_RETRY_NUM; retry++) {
      try (SyncDataNodeInternalServiceClient client = clientManager.borrowClient(endPoint)) {
        return executeSyncRequest(requestType, client, req);
      } catch (Exception e) {
        lastException = e;
        if (retry != DEFAULT_RETRY_NUM - 1) {
          LOGGER.warn("{} failed on DataNode {}, retrying {}...", requestType, endPoint, retry + 1);
          doRetryWait(retry);
        }
      }
    }
    LOGGER.error("{} failed on DataNode {}", requestType, endPoint, lastException);
    return new TSStatus(TSStatusCode.INTERNAL_REQUEST_RETRY_ERROR.getStatusCode())
        .setMessage("All retry failed due to: " + lastException.getMessage());
  }

  public Object sendSyncRequestToDataNodeWithGivenRetry(
      TEndPoint endPoint, Object req, CnToDnSyncRequestType requestType, int retryNum) {
    Throwable lastException = new TException();
    for (int retry = 0; retry < retryNum; retry++) {
      try (SyncDataNodeInternalServiceClient client = clientManager.borrowClient(endPoint)) {
        return executeSyncRequest(requestType, client, req);
      } catch (Exception e) {
        lastException = e;
        if (retry != retryNum - 1) {
          LOGGER.warn("{} failed on DataNode {}, retrying {}...", requestType, endPoint, retry + 1);
          doRetryWait(retry);
        }
      }
    }
    LOGGER.error("{} failed on DataNode {}", requestType, endPoint, lastException);
    return new TSStatus(TSStatusCode.INTERNAL_REQUEST_RETRY_ERROR.getStatusCode())
        .setMessage("All retry failed due to: " + lastException.getMessage());
  }

  private Object executeSyncRequest(
      CnToDnSyncRequestType requestType, SyncDataNodeInternalServiceClient client, Object req)
      throws Exception {
    return Objects.requireNonNull(actionMap.get(requestType)).apply(req, client);
  }

  private void doRetryWait(int retryNum) {
    try {
      if (retryNum < 3) {
        TimeUnit.MILLISECONDS.sleep(800L);
      } else if (retryNum < 5) {
        TimeUnit.MILLISECONDS.sleep(100L * (long) Math.pow(2, retryNum));
      } else {
        TimeUnit.MILLISECONDS.sleep(3200L);
      }
    } catch (InterruptedException e) {
      LOGGER.warn("Retry wait failed.", e);
      Thread.currentThread().interrupt();
    }
  }

  /**
   * change a region leader from the datanode to other datanode, other datanode should be in same
   * raft group.
   *
   * @param regionId the region which will changer leader
   * @param dataNode data node server, change regions leader from it
   * @param newLeaderNode target data node, change regions leader to it
   * @return the result of the change leader operation
   */
  public TRegionLeaderChangeResp changeRegionLeader(
      TConsensusGroupId regionId, TEndPoint dataNode, TDataNodeLocation newLeaderNode) {
    LOGGER.info("Send RPC to data node: {} for changing regions leader on it", dataNode);
    TSStatus status;
    try (SyncDataNodeInternalServiceClient client = clientManager.borrowClient(dataNode)) {
      TRegionLeaderChangeReq req = new TRegionLeaderChangeReq(regionId, newLeaderNode);
      return client.changeRegionLeader(req);
    } catch (ClientManagerException e) {
      LOGGER.error("Can't connect to Data node: {}", dataNode, e);
      status = new TSStatus(TSStatusCode.CAN_NOT_CONNECT_DATANODE.getStatusCode());
      status.setMessage(e.getMessage());
    } catch (TException e) {
      LOGGER.error("Change regions leader error on Date node: {}", dataNode, e);
      status = new TSStatus(TSStatusCode.REGION_LEADER_CHANGE_ERROR.getStatusCode());
      status.setMessage(e.getMessage());
    }
    return new TRegionLeaderChangeResp(status, -1L);
  }

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
