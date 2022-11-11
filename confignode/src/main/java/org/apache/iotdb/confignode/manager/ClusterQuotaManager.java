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
package org.apache.iotdb.confignode.manager;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSetSpaceQuotaReq;
import org.apache.iotdb.confignode.client.DataNodeRequestType;
import org.apache.iotdb.confignode.client.async.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.async.handlers.AsyncClientHandler;
import org.apache.iotdb.confignode.consensus.request.write.quota.SetSpaceQuotaPlan;
import org.apache.iotdb.confignode.persistence.quota.QuotaInfo;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

// TODO: Manage quotas for storage groups
public class ClusterQuotaManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterQuotaManager.class);

  private final IManager configManager;
  private final QuotaInfo quotaInfo;

  public ClusterQuotaManager(IManager configManager, QuotaInfo quotaInfo) {
    this.configManager = configManager;
    this.quotaInfo = quotaInfo;
  }

  public TSStatus setSpaceQuota(TSetSpaceQuotaReq req) {
    ConsensusWriteResponse response =
        configManager
            .getConsensusManager()
            .write(new SetSpaceQuotaPlan(req.getStorageGroup(), req.getSpaceLimit()));
    if (response.getStatus() != null) {
      if (response.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        Map<Integer, TDataNodeLocation> dataNodeLocationMap =
            configManager.getNodeManager().getRegisteredDataNodeLocations();
        AsyncClientHandler<TSetSpaceQuotaReq, TSStatus> clientHandler =
            new AsyncClientHandler<>(DataNodeRequestType.SET_SPACE_QUOTA, req, dataNodeLocationMap);
        AsyncDataNodeClientPool.getInstance().sendAsyncRequestToDataNodeWithRetry(clientHandler);
        return RpcUtils.squashResponseStatusList(clientHandler.getResponseList());
      }
      return response.getStatus();
    } else {
      LOGGER.warn(
          "Unexpected error happened while setting space quota on {}: ",
          req.getStorageGroup().toString(),
          response.getException());
      // consensus layer related errors
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(response.getErrorMessage());
      return res;
    }
  }
}
