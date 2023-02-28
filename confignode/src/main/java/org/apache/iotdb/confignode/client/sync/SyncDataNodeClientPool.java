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
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.mpp.rpc.thrift.TCreateDataRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreatePeerReq;
import org.apache.iotdb.mpp.rpc.thrift.TCreateSchemaRegionReq;
import org.apache.iotdb.mpp.rpc.thrift.TDeleteModelMetricsReq;
import org.apache.iotdb.mpp.rpc.thrift.TDisableDataNodeReq;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidateCacheReq;
import org.apache.iotdb.mpp.rpc.thrift.TInvalidatePermissionCacheReq;
import org.apache.iotdb.mpp.rpc.thrift.TMaintainPeerReq;
import org.apache.iotdb.mpp.rpc.thrift.TRegionLeaderChangeReq;
import org.apache.iotdb.mpp.rpc.thrift.TUpdateTemplateReq;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/** Synchronously send RPC requests to DataNodes. See mpp.thrift for more details. */
public class SyncDataNodeClientPool {

  private static final Logger LOGGER = LoggerFactory.getLogger(SyncDataNodeClientPool.class);

  private static final int DEFAULT_RETRY_NUM = 6;

  private final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient> clientManager;

  private SyncDataNodeClientPool() {
    clientManager =
        new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
            .createClientManager(
                new ClientPoolFactory.SyncDataNodeInternalServiceClientPoolFactory());
  }

  public TSStatus sendSyncRequestToDataNodeWithRetry(
      TEndPoint endPoint, Object req, DataNodeRequestType requestType) {
    Throwable lastException = new TException();
    for (int retry = 0; retry < DEFAULT_RETRY_NUM; retry++) {
      try (SyncDataNodeInternalServiceClient client = clientManager.borrowClient(endPoint)) {
        return executeSyncRequest(requestType, client, req);
      } catch (ClientManagerException | TException e) {
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

  public TSStatus sendSyncRequestToDataNodeWithGivenRetry(
      TEndPoint endPoint, Object req, DataNodeRequestType requestType, int retryNum) {
    Throwable lastException = new TException();
    for (int retry = 0; retry < retryNum; retry++) {
      try (SyncDataNodeInternalServiceClient client = clientManager.borrowClient(endPoint)) {
        return executeSyncRequest(requestType, client, req);
      } catch (ClientManagerException | TException e) {
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

  private TSStatus executeSyncRequest(
      DataNodeRequestType requestType, SyncDataNodeInternalServiceClient client, Object req)
      throws TException {
    switch (requestType) {
      case INVALIDATE_PARTITION_CACHE:
        return client.invalidatePartitionCache((TInvalidateCacheReq) req);
      case INVALIDATE_SCHEMA_CACHE:
        return client.invalidateSchemaCache((TInvalidateCacheReq) req);
      case CREATE_SCHEMA_REGION:
        return client.createSchemaRegion((TCreateSchemaRegionReq) req);
      case CREATE_DATA_REGION:
        return client.createDataRegion((TCreateDataRegionReq) req);
      case DELETE_REGION:
        return client.deleteRegion((TConsensusGroupId) req);
      case INVALIDATE_PERMISSION_CACHE:
        return client.invalidatePermissionCache((TInvalidatePermissionCacheReq) req);
      case DISABLE_DATA_NODE:
        return client.disableDataNode((TDisableDataNodeReq) req);
      case STOP_DATA_NODE:
        return client.stopDataNode();
      case SET_SYSTEM_STATUS:
        return client.setSystemStatus((String) req);
      case KILL_QUERY_INSTANCE:
        return client.killQueryInstance((String) req);
      case UPDATE_TEMPLATE:
        return client.updateTemplate((TUpdateTemplateReq) req);
      case CREATE_NEW_REGION_PEER:
        return client.createNewRegionPeer((TCreatePeerReq) req);
      case ADD_REGION_PEER:
        return client.addRegionPeer((TMaintainPeerReq) req);
      case REMOVE_REGION_PEER:
        return client.removeRegionPeer((TMaintainPeerReq) req);
      case DELETE_OLD_REGION_PEER:
        return client.deleteOldRegionPeer((TMaintainPeerReq) req);
      case DELETE_MODEL_METRICS:
        return client.deleteModelMetrics((TDeleteModelMetricsReq) req);
      default:
        return RpcUtils.getStatus(
            TSStatusCode.EXECUTE_STATEMENT_ERROR, "Unknown request type: " + requestType);
    }
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
      LOGGER.error("Retry wait failed.", e);
    }
  }

  /**
   * change a region leader from the datanode to other datanode, other datanode should be in same
   * raft group
   *
   * @param regionId the region which will changer leader
   * @param dataNode data node server, change regions leader from it
   * @param newLeaderNode target data node, change regions leader to it
   * @return TSStatus
   */
  public TSStatus changeRegionLeader(
      TConsensusGroupId regionId, TEndPoint dataNode, TDataNodeLocation newLeaderNode) {
    LOGGER.info("Send RPC to data node: {} for changing regions leader on it", dataNode);
    TSStatus status;
    try (SyncDataNodeInternalServiceClient client = clientManager.borrowClient(dataNode)) {
      TRegionLeaderChangeReq req = new TRegionLeaderChangeReq(regionId, newLeaderNode);
      status = client.changeRegionLeader(req);
    } catch (ClientManagerException e) {
      LOGGER.error("Can't connect to Data node: {}", dataNode, e);
      status = new TSStatus(TSStatusCode.CAN_NOT_CONNECT_DATANODE.getStatusCode());
      status.setMessage(e.getMessage());
    } catch (TException e) {
      LOGGER.error("Change regions leader error on Date node: {}", dataNode, e);
      status = new TSStatus(TSStatusCode.REGION_LEADER_CHANGE_ERROR.getStatusCode());
      status.setMessage(e.getMessage());
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
