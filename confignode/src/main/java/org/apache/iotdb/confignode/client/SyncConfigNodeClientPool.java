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

  private static final int retryNum = 5;

  private final IClientManager<TEndPoint, SyncConfigNodeIServiceClient> clientManager;

  private TEndPoint configNodeLeader;

  private SyncConfigNodeClientPool() {
    clientManager =
        new IClientManager.Factory<TEndPoint, SyncConfigNodeIServiceClient>()
            .createClientManager(
                new DataNodeClientPoolFactory.SyncConfigNodeIServiceClientPoolFactory());
    configNodeLeader = new TEndPoint();
  }

  private boolean updateConfigNodeLeader(TSStatus status) {
    if (status.getCode() == TSStatusCode.NEED_REDIRECTION.getStatusCode()) {
      if (status.isSetRedirectNode()) {
        configNodeLeader = status.getRedirectNode();
      } else {
        configNodeLeader = null;
      }
      return true;
    }
    return false;
  }

  public TConfigNodeConfigurationResp getConfigNodeConfiguration(
      TConfigNodeLocation configNodeLocation) {
    // TODO: Unified retry logic
    for (int retry = 0; retry < retryNum; retry++) {
      try (SyncConfigNodeIServiceClient client =
          clientManager.borrowClient(configNodeLocation.getInternalEndPoint())) {
        return client.getConfigNodeConfiguration(configNodeLocation);
      } catch (Exception e) {
        LOGGER.warn("Get ConfigNode configuration failed, retrying...", e);
        doRetryWait();
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
      List<TEndPoint> endPointList, TConfigNodeRegisterReq req) {
    // TODO: Unified retry logic
    for (TEndPoint endPoint : endPointList) {
      for (int retry = 0; retry < retryNum; retry++) {
        try (SyncConfigNodeIServiceClient client = clientManager.borrowClient(endPoint)) {
          TConfigNodeRegisterResp resp = client.registerConfigNode(req);
          if (updateConfigNodeLeader(resp.status)) {
            client.close();
            return clientManager.borrowClient(configNodeLeader).registerConfigNode(req);
          } else {
            return resp;
          }
        } catch (Exception e) {
          LOGGER.warn("Register ConfigNode failed, retrying...", e);
          doRetryWait();
        }
      }
    }
    LOGGER.error("Register ConfigNode failed");
    return new TConfigNodeRegisterResp()
        .setStatus(
            new TSStatus(TSStatusCode.ALL_RETRY_FAILED.getStatusCode())
                .setMessage("All retry failed."));
  }

  public TSStatus applyConfigNode(
      List<TEndPoint> endPointList, TConfigNodeLocation configNodeLocation) {
    // TODO: Unified retry logic
    for (TEndPoint endPoint : endPointList) {
      for (int retry = 0; retry < retryNum; retry++) {
        try (SyncConfigNodeIServiceClient client = clientManager.borrowClient(endPoint)) {
          TSStatus status = client.applyConfigNode(configNodeLocation);
          if (updateConfigNodeLeader(status)) {
            client.close();
            return clientManager.borrowClient(configNodeLeader).applyConfigNode(configNodeLocation);
          } else {
            return status;
          }
        } catch (Exception e) {
          LOGGER.warn("Apply ConfigNode failed, retrying...", e);
          doRetryWait();
        }
      }
    }
    LOGGER.error("Apply ConfigNode failed");
    return new TSStatus(TSStatusCode.ALL_RETRY_FAILED.getStatusCode())
        .setMessage("All retry failed.");
  }

  public TSStatus removeConfigNode(
      List<TConfigNodeLocation> configNodeLocations, TConfigNodeLocation configNodeLocation) {
    // TODO: Unified retry logic
    for (TConfigNodeLocation nodeLocation : configNodeLocations) {
      if (nodeLocation.equals(configNodeLocation)) {
        continue;
      }

      for (int retry = 0; retry < retryNum; retry++) {
        try (SyncConfigNodeIServiceClient client =
            clientManager.borrowClient(nodeLocation.getInternalEndPoint())) {
          TSStatus status = client.removeConfigNode(configNodeLocation);
          if (updateConfigNodeLeader(status)) {
            client.close();
            return clientManager
                .borrowClient(configNodeLeader)
                .removeConfigNode(configNodeLocation);
          } else {
            return status;
          }
        } catch (Exception e) {
          LOGGER.warn("Remove ConfigNode failed, retrying...", e);
          doRetryWait();
        }
      }
    }

    LOGGER.error("Remove ConfigNode failed");
    return new TSStatus(TSStatusCode.ALL_RETRY_FAILED.getStatusCode())
        .setMessage("All retry failed.");
  }

  /** Only use stopConfigNode when the ConfigNode is removed. */
  public TSStatus stopConfigNode(TConfigNodeLocation configNodeLocation) {
    // TODO: Unified retry logic
    for (int retry = 0; retry < retryNum; retry++) {
      try (SyncConfigNodeIServiceClient client =
          clientManager.borrowClient(configNodeLocation.getInternalEndPoint())) {
        return client.removeConfigNode(configNodeLocation);
      } catch (Exception e) {
        LOGGER.warn("Stop ConfigNode failed, retrying...", e);
        doRetryWait();
      }
    }
    LOGGER.error("Stop ConfigNode failed");
    return new TSStatus(TSStatusCode.ALL_RETRY_FAILED.getStatusCode())
        .setMessage("All retry failed.");
  }

  private void doRetryWait() {
    try {
      TimeUnit.MILLISECONDS.sleep(100);
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
