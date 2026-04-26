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
package com.timecho.iotdb.service;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TLicense;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.partition.DataPartition;
import org.apache.iotdb.commons.partition.DataPartitionQueryParam;
import org.apache.iotdb.commons.utils.TimePartitionUtils;
import org.apache.iotdb.confignode.rpc.thrift.TShowActivationResp;
import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.thrift.impl.ClientRPCServiceImpl;
import org.apache.iotdb.db.utils.DateTimeUtils;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.LicenseInfoResp;
import org.apache.iotdb.service.rpc.thrift.TTableDeviceLeaderReq;
import org.apache.iotdb.service.rpc.thrift.TTableDeviceLeaderResp;

import org.apache.thrift.TException;
import org.apache.tsfile.file.metadata.IDeviceID;

import java.util.Collections;

public class ClientRPCServiceImplNew extends ClientRPCServiceImpl {

  public static final String ROOT_USER = "root";

  @Override
  public LicenseInfoResp getLicenseInfo() throws TException {
    try (ConfigNodeClient configNodeClient =
        ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      TShowActivationResp resp = configNodeClient.showActivation();
      TLicense license = resp.getLicense();
      TLicense usage = resp.getUsage();
      return new LicenseInfoResp(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()))
          .setIsActive(!CommonDescriptor.getInstance().getConfig().isUnactivated())
          .setExpireDate(
              DateTimeUtils.convertMillsecondToZonedDateTime(license.expireTimestamp).toString())
          .setIsEnterprise(true)
          .setLicense(license)
          .setUsage(usage);
    } catch (ClientManagerException e) {
      throw new TException(e);
    }
  }

  @Override
  public TTableDeviceLeaderResp fetchDeviceLeader(TTableDeviceLeaderReq req) throws TException {
    IClientSession clientSession = SESSION_MANAGER.getCurrSessionAndUpdateIdleTime();
    if (!SESSION_MANAGER.checkLogin(clientSession)) {
      return new TTableDeviceLeaderResp(getNotLoggedInStatus(), "", "");
    }
    int startIndex = 0;
    String[] trueSegments = new String[req.getIsSetTagSize()];
    for (int i = 0; i < req.getIsSetTagSize(); i++) {
      trueSegments[i] =
          Boolean.TRUE.equals(req.getIsSetTag().get(i))
              ? req.getDeviceId().get(startIndex++)
              : null;
      if (i == 0) {
        // Always convert table name to lower case
        trueSegments[i] = trueSegments[i].toLowerCase();
      }
    }
    TTableDeviceLeaderResp resp = new TTableDeviceLeaderResp();
    resp.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    IDeviceID deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create(trueSegments);
    TTimePartitionSlot timePartitionSlot = TimePartitionUtils.getTimePartitionSlot(req.getTime());
    DataPartitionQueryParam queryParam =
        new DataPartitionQueryParam(deviceID, Collections.singletonList(timePartitionSlot));
    try {
      DataPartition dataPartition =
          partitionFetcher.getOrCreateDataPartition(
              Collections.singletonMap(req.getDbName(), Collections.singletonList(queryParam)));
      TRegionReplicaSet targetRegionReplicaSet =
          dataPartition.getAllReplicaSets().stream().findFirst().orElse(null);
      TEndPoint targetEndPoint =
          targetRegionReplicaSet != null
              ? targetRegionReplicaSet.getDataNodeLocations().get(0).getClientRpcEndPoint()
              : null;
      if (targetEndPoint != null) {
        resp.setIp(targetEndPoint.getIp());
        resp.setPort(String.valueOf(targetEndPoint.getPort()));
      } else {
        resp.setIp("");
        resp.setPort("");
      }
    } catch (StatementAnalyzeException e) {
      resp.setStatus(
          RpcUtils.getStatus(
              TSStatusCode.EXECUTE_STATEMENT_ERROR,
              "Failed to fetch device leader: " + e.getMessage()));
      resp.setIp("");
      resp.setPort("");
    }
    return resp;
  }
}
