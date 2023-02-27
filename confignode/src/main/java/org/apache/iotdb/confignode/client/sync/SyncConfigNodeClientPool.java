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

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.client.ConfigNodeRequestType;
import org.apache.iotdb.confignode.rpc.thrift.TAddConsensusGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRestartReq;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/** Synchronously send RPC requests to ConfigNode. See confignode.thrift for more details. */
public class SyncConfigNodeClientPool {

  private static final Logger LOGGER = LoggerFactory.getLogger(SyncConfigNodeClientPool.class);

  private static final int MAX_RETRY_NUM = 6;

  private final IClientManager<TEndPoint, SyncConfigNodeIServiceClient> clientManager;

  private TEndPoint configNodeLeader;

  private SyncConfigNodeClientPool() {
    clientManager =
        new IClientManager.Factory<TEndPoint, SyncConfigNodeIServiceClient>()
            .createClientManager(new ClientPoolFactory.SyncConfigNodeIServiceClientPoolFactory());
    configNodeLeader = new TEndPoint();
  }

  private void updateConfigNodeLeader(TSStatus status) {
    if (status.isSetRedirectNode()) {
      configNodeLeader = status.getRedirectNode();
    } else {
      configNodeLeader = null;
    }
  }

  public Object sendSyncRequestToConfigNodeWithRetry(
      TEndPoint endPoint, Object req, ConfigNodeRequestType requestType) {

    Throwable lastException = null;
    for (int retry = 0; retry < MAX_RETRY_NUM; retry++) {
      try (SyncConfigNodeIServiceClient client = clientManager.borrowClient(endPoint)) {
        switch (requestType) {
          case REGISTER_CONFIG_NODE:
            // Only use registerConfigNode when the ConfigNode is first startup.
            return client.registerConfigNode((TConfigNodeRegisterReq) req);
          case ADD_CONSENSUS_GROUP:
            return client.addConsensusGroup((TAddConsensusGroupReq) req);
          case NOTIFY_REGISTER_SUCCESS:
            client.notifyRegisterSuccess();
            return null;
          case RESTART_CONFIG_NODE:
            return client.restartConfigNode((TConfigNodeRestartReq) req);
          case REMOVE_CONFIG_NODE:
            return removeConfigNode((TConfigNodeLocation) req, client);
          case DELETE_CONFIG_NODE_PEER:
            return client.deleteConfigNodePeer((TConfigNodeLocation) req);
          case REPORT_CONFIG_NODE_SHUTDOWN:
            return client.reportConfigNodeShutdown((TConfigNodeLocation) req);
          case STOP_CONFIG_NODE:
            // Only use stopConfigNode when the ConfigNode is removed.
            return client.stopConfigNode((TConfigNodeLocation) req);
          default:
            return RpcUtils.getStatus(
                TSStatusCode.EXECUTE_STATEMENT_ERROR, "Unknown request type: " + requestType);
        }
      } catch (Exception e) {
        lastException = e;
        LOGGER.warn(
            "{} failed on ConfigNode {}, because {}, retrying {}...",
            requestType,
            endPoint,
            e.getMessage(),
            retry);
        doRetryWait(retry);
      }
    }
    LOGGER.error("{} failed on ConfigNode {}", requestType, endPoint, lastException);
    return RpcUtils.getStatus(
        TSStatusCode.INTERNAL_REQUEST_RETRY_ERROR,
        "All retry failed due to: " + lastException.getMessage());
  }

  /**
   * ConfigNode Leader stop any ConfigNode in the cluster
   *
   * @param configNodeLocation To be removed ConfigNode
   * @return SUCCESS_STATUS: remove ConfigNode success, other status remove failed
   */
  public TSStatus removeConfigNode(
      TConfigNodeLocation configNodeLocation, SyncConfigNodeIServiceClient client)
      throws ClientManagerException, TException, InterruptedException {
    TSStatus status = client.removeConfigNode(configNodeLocation);
    while (status.getCode() == TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode()) {
      TimeUnit.MILLISECONDS.sleep(2000);
      updateConfigNodeLeader(status);
      try (SyncConfigNodeIServiceClient clientLeader =
          clientManager.borrowClient(configNodeLeader)) {
        status = clientLeader.removeConfigNode(configNodeLocation);
      }
    }
    return status;
  }

  private void doRetryWait(int retryNum) {
    try {
      TimeUnit.MILLISECONDS.sleep(100L * (long) Math.pow(2, retryNum));
    } catch (InterruptedException e) {
      LOGGER.error("Retry wait failed.", e);
    }
  }

  // TODO: Is the ClientPool must be a singleton?
  private static class SyncConfigNodeClientPoolHolder {

    private static final SyncConfigNodeClientPool INSTANCE = new SyncConfigNodeClientPool();

    private SyncConfigNodeClientPoolHolder() {
      // Empty constructor
    }
  }

  public static SyncConfigNodeClientPool getInstance() {
    return SyncConfigNodeClientPoolHolder.INSTANCE;
  }
}
