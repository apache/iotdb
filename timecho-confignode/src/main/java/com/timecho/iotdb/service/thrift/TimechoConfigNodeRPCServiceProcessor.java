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

package com.timecho.iotdb.service.thrift;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.rpc.thrift.TActivationControl;
import org.apache.iotdb.confignode.rpc.thrift.TCheckMaxClientNumResp;
import org.apache.iotdb.confignode.rpc.thrift.TCheckSessionNumReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeHeartbeatReq;
import org.apache.iotdb.confignode.rpc.thrift.TConfigNodeHeartbeatResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllActivationStatusResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowActivationResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowSystemInfoResp;
import org.apache.iotdb.confignode.service.thrift.ConfigNodeRPCServiceProcessor;
import org.apache.iotdb.rpc.TSStatusCode;

import com.timecho.iotdb.manager.TimechoConfigManager;
import com.timecho.iotdb.manager.regulate.RegulateManager;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class TimechoConfigNodeRPCServiceProcessor extends ConfigNodeRPCServiceProcessor {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(TimechoConfigNodeRPCServiceProcessor.class);
  private final TimechoConfigManager configManager;

  public TimechoConfigNodeRPCServiceProcessor(TimechoConfigManager configManager) {
    super(configManager);
    this.configManager = configManager;
  }

  @Override
  public TConfigNodeHeartbeatResp getConfigNodeHeartBeat(TConfigNodeHeartbeatReq heartbeatReq) {
    TConfigNodeHeartbeatResp resp = new TConfigNodeHeartbeatResp();
    resp.setTimestamp(heartbeatReq.getTimestamp());
    if (heartbeatReq.isSetActivationControl()) {
      if (Objects.equals(
          heartbeatReq.getActivationControl(), TActivationControl.ALL_LICENSE_FILE_DELETED)) {
        configManager
            .getActivationManager()
            .giveUpLicenseBecauseLeaderBelieveThereIsNoActiveNodeInCluster();
      } else {
        throw new UnsupportedOperationException(
            String.format("%s is not supported", heartbeatReq.getActivationControl()));
      }
    }
    configManager.getActivationManager().tryLoadRemoteLicense(heartbeatReq.getLicence());
    resp.setActivateStatus(configManager.getActivationManager().getActivateStatus().toString());
    if (configManager.getActivationManager().isActive()) {
      // Report my license only if I'm active
      resp.setLicense(configManager.getActivationManager().getLicense().toTLicense());
    }
    return resp;
  }

  @Override
  public TSStatus setLicenseFile(String fileName, String content) throws TException {
    return configManager.getActivationManager().setLicenseFile(fileName, content);
  }

  @Override
  public TSStatus deleteLicenseFile(String fileName) throws TException {
    return configManager.getActivationManager().deleteLicenseFile(fileName);
  }

  @Override
  public TSStatus getLicenseFile(String fileName) throws TException {
    return configManager.getActivationManager().getLicenseFile(fileName);
  }

  @Override
  public TShowSystemInfoResp showSystemInfo() throws TException {
    IClientManager<TEndPoint, SyncConfigNodeIServiceClient> clientManager =
        new IClientManager.Factory<TEndPoint, SyncConfigNodeIServiceClient>()
            .createClientManager(new ClientPoolFactory.SyncConfigNodeIServiceClientPoolFactory());
    List<TConfigNodeLocation> locations = configManager.getNodeManager().getRegisteredConfigNodes();
    locations.sort(Comparator.comparingInt(TConfigNodeLocation::getConfigNodeId));
    TShowSystemInfoResp resp = new TShowSystemInfoResp();
    List<String> systemInfoList = new ArrayList<>();
    try {
      for (TConfigNodeLocation location : locations) {
        if (location.getConfigNodeId()
            == ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId()) {
          systemInfoList.add(RegulateManager.generateSystemInfoContentWithVersion());
        } else {
          try (SyncConfigNodeIServiceClient client =
              clientManager.borrowClient(location.getInternalEndPoint())) {
            systemInfoList.add(client.getSystemInfo());
          }
        }
      }
    } catch (Exception e) {
      resp.setStatus(new TSStatus(TSStatusCode.LICENSE_ERROR.getStatusCode()));
      return resp;
    }
    resp.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    resp.setSystemInfoList(systemInfoList);
    return resp;
  }

  @Override
  public String getSystemInfo() throws TException {
    try {
      return RegulateManager.generateSystemInfoContentWithVersion();
    } catch (Exception e) {
      throw new TException(e);
    }
  }

  @Override
  public TShowActivationResp cliActivate(List<String> licenseList) throws TException {
    return configManager.cliActivate(licenseList);
  }

  @Override
  public TSStatus checkSystemInfo(String license) throws TException {
    return configManager.checkSystemInfo(license);
  }

  @Override
  public TShowActivationResp showActivation() throws TException {
    return configManager.showActivation();
  }

  @Override
  public TSStatus getActivateStatus() throws TException {
    TSStatus result = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    result.setMessage(configManager.getActivationManager().getActivateStatus().toString());
    return result;
  }

  @TestOnly
  @Override
  public TGetAllActivationStatusResp getAllActivationStatus() throws TException {
    TGetAllActivationStatusResp resp = new TGetAllActivationStatusResp();
    resp.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    Map<Integer, String> activationMap = configManager.getLoadManager().getNodeActivateStatus();
    resp.setActivationStatusMap(activationMap);
    return resp;
  }

  @Override
  public TSStatus checkSessionNum(TCheckSessionNumReq req) {
    return configManager.checkSessionNumOnConnect(
        req.getCurrentSessionInfo(), req.getRpcMaxConcurrentClientNum());
  }

  @Override
  public TCheckMaxClientNumResp checkMaxClientNumValid(int maxConcurrentClientNum) {
    return configManager.checkMaxClientNumValid(maxConcurrentClientNum);
  }

  @Override
  public String getLocalSystemInfo() {
    return configManager.getLocalSystemInfo();
  }
}
