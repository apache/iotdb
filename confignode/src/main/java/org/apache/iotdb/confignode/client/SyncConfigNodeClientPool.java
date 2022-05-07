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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeRegisterResp;
import org.apache.iotdb.db.client.DataNodeClientPoolFactory;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/** Synchronously send RPC requests to ConfigNode. See confignode.thrift for more details. */
public class SyncConfigNodeClientPool {

  private static final Logger LOGGER = LoggerFactory.getLogger(SyncConfigNodeClientPool.class);

  private static final int retryNum = 5;

  private final IClientManager<TEndPoint, SyncConfigNodeIServiceClient> clientManager;

  private SyncConfigNodeClientPool() {
    clientManager =
        new IClientManager.Factory<TEndPoint, SyncConfigNodeIServiceClient>()
            .createClientManager(
                new DataNodeClientPoolFactory.SyncConfigNodeIServiceClientPoolFactory());
  }

  /** Only use registerConfigNode when the ConfigNode is first startup. */
  public TConfigNodeRegisterResp registerConfigNode(
      TEndPoint endPoint, TConfigNodeRegisterReq req) {
    // TODO: Unified retry logic
    for (int retry = 0; retry < retryNum; retry++) {
      try (SyncConfigNodeIServiceClient client = clientManager.borrowClient(endPoint)) {
        return client.registerConfigNode(req);
      } catch (Exception e) {
        LOGGER.warn("Register ConfigNode failed, retrying...", e);
        doRetryWait();
      }
    }
    LOGGER.error("Register ConfigNode failed");
    return new TConfigNodeRegisterResp()
        .setStatus(
            new TSStatus(TSStatusCode.ALL_RETRY_FAILED.getStatusCode())
                .setMessage("All retry failed."));
  }

  public TSStatus applyConfigNode(TEndPoint endPoint, TConfigNodeLocation configNodeLocation) {
    // TODO: Unified retry logic
    for (int retry = 0; retry < retryNum; retry++) {
      try (SyncConfigNodeIServiceClient client = clientManager.borrowClient(endPoint)) {
        return client.applyConfigNode(configNodeLocation);
      } catch (Exception e) {
        LOGGER.warn("Apply ConfigNode failed, retrying...", e);
        doRetryWait();
      }
    }
    LOGGER.error("Apply ConfigNode failed");
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
