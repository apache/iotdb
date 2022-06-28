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

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeConfigurationResp;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterResp;
import org.apache.iotdb.db.client.DataNodeClientPoolFactory;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

/** Synchronously send RPC requests to ConfigNode. See confignode.thrift for more details. */
public class SyncConfigNodeClientPool {

  private static final Logger LOGGER = LoggerFactory.getLogger(SyncConfigNodeClientPool.class);

  private static final int retryNum = 6;

  private final IClientManager<TEndPoint, SyncConfigNodeIServiceClient> clientManager;

  private TEndPoint configNodeLeader;

  private SyncConfigNodeClientPool() {
    clientManager =
        new IClientManager.Factory<TEndPoint, SyncConfigNodeIServiceClient>()
            .createClientManager(
                new DataNodeClientPoolFactory.SyncConfigNodeIServiceClientPoolFactory());
    configNodeLeader = new TEndPoint();
  }

  private void updateConfigNodeLeader(TSStatus status) {
    if (status.isSetRedirectNode()) {
      configNodeLeader = status.getRedirectNode();
    } else {
      configNodeLeader = null;
    }
  }

  /** Get target ConfigNode configuration */
  public TConfigNodeConfigurationResp getConfigNodeConfiguration(
      TConfigNodeLocation configNodeLocation) {
    // TODO: Unified retry logic
    for (int retry = 0; retry < retryNum; retry++) {
      try (SyncConfigNodeIServiceClient client =
          clientManager.borrowClient(configNodeLocation.getInternalEndPoint())) {
        return client.getConfigNodeConfiguration(configNodeLocation);
      } catch (Exception e) {
        LOGGER.warn("Get ConfigNode configuration failed, retrying...", e);
        doRetryWait(retry);
      }
    }
    LOGGER.error("Get ConfigNode configuration failed");
    return new TConfigNodeConfigurationResp()
        .setStatus(
            new TSStatus(TSStatusCode.ALL_RETRY_FAILED.getStatusCode())
                .setMessage("All retry failed."));
  }

  /** Only use registerConfigNode when the ConfigNode is first startup. */
  public TConfigNodeRegisterResp registerConfigNode(
      TEndPoint endPoint, TConfigNodeRegisterReq req) {
    // TODO: Unified retry logic
    Throwable lastException = null;
    for (int retry = 0; retry < retryNum; retry++) {
      try (SyncConfigNodeIServiceClient client = clientManager.borrowClient(endPoint)) {
        return client.registerConfigNode(req);
      } catch (Exception e) {
        lastException = e;
        LOGGER.warn("Register ConfigNode failed because {}, retrying {}...", e.getMessage(), retry);
        doRetryWait(retry);
      }
    }
    LOGGER.error("Register ConfigNode failed", lastException);
    return new TConfigNodeRegisterResp()
        .setStatus(
            new TSStatus(TSStatusCode.ALL_RETRY_FAILED.getStatusCode())
                .setMessage("All retry failed due to " + lastException.getMessage()));
  }

  public TSStatus addConsensusGroup(
      TEndPoint endPoint, List<TConfigNodeLocation> configNodeLocation) {
    // TODO: Unified retry logic
    Throwable lastException = null;
    for (int retry = 0; retry < retryNum; retry++) {
      try (SyncConfigNodeIServiceClient client = clientManager.borrowClient(endPoint)) {
        TConfigNodeRegisterResp registerResp = new TConfigNodeRegisterResp();
        registerResp.setConfigNodeList(configNodeLocation);
        registerResp.setStatus(StatusUtils.OK);
        return client.addConsensusGroup(registerResp);
      } catch (Throwable e) {
        lastException = e;
        LOGGER.warn(
            "Add Consensus Group failed because {}, retrying {} ...", e.getMessage(), retry);
        doRetryWait(retry);
      }
    }
    LOGGER.error("Add ConsensusGroup failed", lastException);
    return new TSStatus(TSStatusCode.ALL_RETRY_FAILED.getStatusCode())
        .setMessage("All retry failed due to " + lastException.getMessage());
  }

  /**
   * ConfigNode Leader stop any ConfigNode in the cluster
   *
   * @param configNodeLocations confignode_list of confignode-system.properties
   * @param configNodeLocation To be removed ConfigNode
   * @return SUCCESS_STATUS: remove ConfigNode success, other status remove failed
   */
  public TSStatus removeConfigNode(
      List<TConfigNodeLocation> configNodeLocations, TConfigNodeLocation configNodeLocation) {
    // TODO: Unified retry logic
    Throwable lastException = null;
    for (TConfigNodeLocation nodeLocation : configNodeLocations) {
      for (int retry = 0; retry < retryNum; retry++) {
        try (SyncConfigNodeIServiceClient client =
            clientManager.borrowClient(nodeLocation.getInternalEndPoint())) {
          TSStatus status = client.removeConfigNode(configNodeLocation);
          while (status.getCode() == TSStatusCode.NEED_REDIRECTION.getStatusCode()) {
            TimeUnit.MILLISECONDS.sleep(2000);
            updateConfigNodeLeader(status);
            try (SyncConfigNodeIServiceClient clientLeader =
                clientManager.borrowClient(configNodeLeader)) {
              status = clientLeader.removeConfigNode(configNodeLocation);
            }
          }
          return status;
        } catch (Throwable e) {
          lastException = e;
          LOGGER.warn(
              "Remove ConfigNode failed because {}, retrying {} ...", e.getMessage(), retry);
          doRetryWait(retry);
        }
      }
    }

    LOGGER.error("Remove ConfigNode failed", lastException);
    return new TSStatus(TSStatusCode.ALL_RETRY_FAILED.getStatusCode())
        .setMessage("All retry failed due to " + lastException.getMessage());
  }

  /** Only use stopConfigNode when the ConfigNode is removed. */
  public TSStatus stopConfigNode(TConfigNodeLocation configNodeLocation) {
    // TODO: Unified retry logic
    Throwable lastException = null;
    for (int retry = 0; retry < retryNum; retry++) {
      try (SyncConfigNodeIServiceClient client =
          clientManager.borrowClient(configNodeLocation.getInternalEndPoint())) {
        return client.stopConfigNode(configNodeLocation);
      } catch (Exception e) {
        lastException = e;
        LOGGER.warn("Stop ConfigNode failed because {}, retrying {}...", e.getMessage(), retry);
        doRetryWait(retry);
      }
    }
    LOGGER.error("Stop ConfigNode failed", lastException);
    return new TSStatus(TSStatusCode.ALL_RETRY_FAILED.getStatusCode())
        .setMessage("All retry failed due to" + lastException.getMessage());
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
