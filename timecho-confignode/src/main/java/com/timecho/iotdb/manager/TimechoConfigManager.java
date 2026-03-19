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

package com.timecho.iotdb.manager;

import org.apache.iotdb.common.rpc.thrift.TAINodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TAINodeLocation;
import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSetConfigurationReq;
import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.LicenseException;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.manager.PermissionManager;
import org.apache.iotdb.confignode.manager.ProcedureManager;
import org.apache.iotdb.confignode.persistence.ProcedureInfo;
import org.apache.iotdb.confignode.persistence.auth.AuthorInfo;
import org.apache.iotdb.confignode.persistence.node.NodeInfo;
import org.apache.iotdb.confignode.rpc.thrift.TCheckMaxClientNumResp;
import org.apache.iotdb.confignode.rpc.thrift.TNodeActivateInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowActivationResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowClusterResp;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import com.timecho.iotdb.confignode.procedure.consensus.request.write.auth.EnableSeparationOfAdminPowersPlan;
import com.timecho.iotdb.manager.load.TimechoLoadManager;
import com.timecho.iotdb.manager.node.TimechoNodeManager;
import com.timecho.iotdb.manager.regulate.RegulateManager;
import com.timecho.iotdb.persistence.auth.TimechoAuthorInfo;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TimechoConfigManager extends org.apache.iotdb.confignode.manager.ConfigManager
    implements ITimechoManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(TimechoConfigManager.class);

  private TimechoNodeManager timechoNodeManager;
  private TimechoLoadManager timechoLoadManager;
  protected RegulateManager regulateManager;

  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();

  public TimechoConfigManager() throws IOException, LicenseException {
    super();
    initActivationManager();
  }

  @Override
  protected PermissionManager createPermissionManager(AuthorInfo authorInfo) {
    return new TimechoPermissionManager(this, authorInfo);
  }

  @Override
  protected ProcedureManager createProcedureManager(ProcedureInfo procedureInfo) {
    return new TimechoProcedureManager(this, procedureInfo);
  }

  @Override
  protected AuthorInfo createAuthorInfo() {
    return new TimechoAuthorInfo(this);
  }

  @Override
  protected void setNodeManager(NodeInfo nodeInfo) {
    this.timechoNodeManager = new TimechoNodeManager(this, nodeInfo);
    super.nodeManager = this.timechoNodeManager;
  }

  @Override
  public TimechoNodeManager getNodeManager() {
    return this.timechoNodeManager;
  }

  @Override
  protected void setLoadManager() {
    this.timechoLoadManager = new TimechoLoadManager(this);
    super.loadManager = this.timechoLoadManager;
  }

  @Override
  public TimechoLoadManager getLoadManager() {
    return timechoLoadManager;
  }

  @Override
  public RegulateManager getActivationManager() {
    return regulateManager;
  }

  @Override
  public TShowClusterResp showCluster() {
    TShowClusterResp result = super.showCluster();

    List<TAINodeLocation> aiNodeInfoLocations =
        timechoNodeManager.getRegisteredAINodes().stream()
            .map(TAINodeConfiguration::getLocation)
            .sorted(Comparator.comparingInt(TAINodeLocation::getAiNodeId))
            .collect(Collectors.toList());
    Map<Integer, String> nodeStatusMap = getLoadManager().getNodeStatusWithReason();
    aiNodeInfoLocations.forEach(
        aiNodeLocation ->
            nodeStatusMap.putIfAbsent(aiNodeLocation.getAiNodeId(), NodeStatus.Unknown.toString()));
    result.setAiNodeList(aiNodeInfoLocations);

    Map<Integer, TNodeActivateInfo> nodeActivateInfoMap =
        getLoadManager().getNodeSimplifiedActivateStatus();
    nodeActivateInfoMap.put(
        CONF.getConfigNodeId(),
        new TNodeActivateInfo(regulateManager.getActivateStatus().toSimpleString()));
    result.setNodeActivateInfo(nodeActivateInfoMap);

    return result;
  }

  public TShowActivationResp cliActivate(List<String> licenseList) {
    TSStatus status = confirmLeader();
    if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return new TShowActivationResp(status);
    }
    IClientManager<TEndPoint, SyncConfigNodeIServiceClient> clientManager =
        new IClientManager.Factory<TEndPoint, SyncConfigNodeIServiceClient>()
            .createClientManager(new ClientPoolFactory.SyncConfigNodeIServiceClientPoolFactory());
    List<TConfigNodeLocation> locations = nodeManager.getRegisteredConfigNodes();
    locations.sort(Comparator.comparingInt(TConfigNodeLocation::getConfigNodeId));
    try {
      cliActivateCheckNumberCorrect(licenseList, locations);
      regulateManager.cliActivateCheckLicenseContentAvailable(licenseList);
      cliActivateCheckSystemInfo(licenseList, locations, clientManager);
      cliActivateDistributeLicense(licenseList, locations, clientManager);
      LOGGER.info("[CLI activation] Successfully updated all ConfigNodes' license");
      // wait licenses loaded for a few seconds
      Thread.sleep(3 * ConfigNodeDescriptor.getInstance().getConf().getHeartbeatIntervalInMs());
      return showActivation();
    } catch (Exception e) {
      LOGGER.warn("[CLI activation]", e);
      return new TShowActivationResp(
          new TSStatus(TSStatusCode.LICENSE_ERROR.getStatusCode()).setMessage(e.getMessage()));
    }
  }

  private void cliActivateCheckNumberCorrect(
      List<String> licenseList, List<TConfigNodeLocation> locations) throws LicenseException {
    if (licenseList.size() != locations.size()) {
      throw new LicenseException(
          String.format(
              "%d licenses are provided, but there are %d ConfigNodes in cluster. Those two numbers need to be same. ",
              licenseList.size(), locations.size()));
    }
  }

  private void cliActivateCheckSystemInfo(
      List<String> licenseList,
      List<TConfigNodeLocation> locations,
      IClientManager<TEndPoint, SyncConfigNodeIServiceClient> clientManager)
      throws LicenseException, TException, ClientManagerException {
    TShowActivationResp resp = new TShowActivationResp();
    Iterator<String> licenseIterator = licenseList.iterator();
    List<Integer> failedList = new ArrayList<>();
    for (TConfigNodeLocation location : locations) {
      TSStatus checkSystemInfoResult;
      if (location.getConfigNodeId()
          == ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId()) {
        checkSystemInfoResult = checkSystemInfo(licenseIterator.next());
      } else {
        try (SyncConfigNodeIServiceClient client =
            clientManager.borrowClient(location.getInternalEndPoint())) {
          checkSystemInfoResult = client.checkSystemInfo(licenseIterator.next());
        }
      }
      if (checkSystemInfoResult.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        failedList.add(location.getConfigNodeId());
      }
    }
    if (!failedList.isEmpty()) {
      String failMessage =
          String.format(
              "[CLI activation] Some ConfigNode's system info check failed: %s", failedList);
      LOGGER.warn(failMessage);
      throw new LicenseException(failMessage);
    }
  }

  private void cliActivateDistributeLicense(
      List<String> licenseList,
      List<TConfigNodeLocation> locations,
      IClientManager<TEndPoint, SyncConfigNodeIServiceClient> clientManager)
      throws LicenseException, TException, ClientManagerException {
    Iterator<String> licenseIterator = licenseList.iterator();
    List<Integer> failedList = new ArrayList<>();
    for (TConfigNodeLocation location : locations) {
      TSStatus setLicenseResult;
      if (location.getConfigNodeId()
          == ConfigNodeDescriptor.getInstance().getConf().getConfigNodeId()) {
        setLicenseResult =
            regulateManager.setLicenseFile(
                RegulateManager.LICENSE_FILE_NAME, licenseIterator.next());
      } else {
        try (SyncConfigNodeIServiceClient client =
            clientManager.borrowClient(location.getInternalEndPoint())) {
          setLicenseResult =
              client.setLicenseFile(RegulateManager.LICENSE_FILE_NAME, licenseIterator.next());
        }
      }
      if (setLicenseResult.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        failedList.add(location.getConfigNodeId());
      }
    }
    if (!failedList.isEmpty()) {
      String failMessage =
          String.format(
              "[CLI activation] Some ConfigNodes' license may not be updated successfully: %s",
              failedList);
      LOGGER.warn(failMessage);
      throw new LicenseException(failMessage);
    }
  }

  public TShowActivationResp showActivation() {
    TShowActivationResp resp =
        new TShowActivationResp(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    resp.setLicense(regulateManager.getLicense().toTLicense());
    resp.setUsage(regulateManager.getLicenseUsage());
    resp.setClusterActivationStatus(regulateManager.calculateClusterActivationStatus());
    return resp;
  }

  public TSStatus checkSystemInfo(String license) {
    if (regulateManager.checkLicenseContentAvailable(license)) {
      return RpcUtils.SUCCESS_STATUS;
    }
    return new TSStatus(TSStatusCode.LICENSE_ERROR.getStatusCode());
  }

  protected void initActivationManager() throws LicenseException {
    this.regulateManager = new RegulateManager(this);
  }

  @Override
  public TSStatus setConfiguration(TSetConfigurationReq req) {
    // check special configuration
    String isEnableSeparationOfAdminPower =
        req.getConfigs().get(IoTDBConstant.ENABLE_SEPARATION_OF_ADMIN_POWERS);
    if (isEnableSeparationOfAdminPower != null) {
      if (!Boolean.parseBoolean(isEnableSeparationOfAdminPower)) {
        return RpcUtils.getStatus(
            TSStatusCode.NO_PERMISSION, "No permission to turn off the separation of powers mode");
      }
      TSStatus tsStatus = confirmLeader();
      if (tsStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return tsStatus;
      }
      return ((TimechoProcedureManager) getProcedureManager())
          .enableSeparationOfAdminPowers(
              new EnableSeparationOfAdminPowersPlan(
                  IoTDBConstant.DEFAULT_SYSTEM_ADMIN_USERNAME,
                  IoTDBConstant.DEFAULT_SECURITY_ADMIN_USERNAME,
                  IoTDBConstant.DEFAULT_AUDIT_ADMIN_USERNAME));
    }

    String isEnableEncryptConfigFile =
        req.getConfigs().get(IoTDBConstant.ENABLE_ENCRYPT_CONFIG_FILE);
    if (isEnableEncryptConfigFile != null) {
      if (!Boolean.parseBoolean(isEnableEncryptConfigFile)) {
        return RpcUtils.getStatus(
            TSStatusCode.NO_PERMISSION,
            "No permission to turn off the switch of encrypt config file");
      }
      TSStatus tsStatus = confirmLeader();
      if (tsStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return tsStatus;
      }
    }

    String isEnableEncryptPermissionFile =
        req.getConfigs().get(IoTDBConstant.ENABLE_ENCRYPT_PERMISSION_FILE);
    if (isEnableEncryptPermissionFile != null) {
      if (!Boolean.parseBoolean(isEnableEncryptPermissionFile)) {
        return RpcUtils.getStatus(
            TSStatusCode.NO_PERMISSION,
            "No permission to turn off the switch of encrypt permission file");
      }
      TSStatus tsStatus = confirmLeader();
      if (tsStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return tsStatus;
      }
    }

    return super.setConfiguration(req);
  }

  private static final String DN_RPC_MAX_CONCURRENT_CLIENT_NUM = "dn_rpc_max_concurrent_client_num";

  @Override
  protected TSStatus validateConfigurationBeforeSet(TSetConfigurationReq req) {
    String value = req.getConfigs().get(DN_RPC_MAX_CONCURRENT_CLIENT_NUM);
    if (value == null) {
      return null;
    }
    int n;
    try {
      n = Integer.parseInt(value.trim());
    } catch (NumberFormatException e) {
      return RpcUtils.getStatus(
          TSStatusCode.EXECUTE_STATEMENT_ERROR,
          "Invalid value for " + DN_RPC_MAX_CONCURRENT_CLIENT_NUM + ": " + value);
    }
    TCheckMaxClientNumResp resp = checkMaxClientNumValid(n);
    if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      int minSum = resp.getMinSessionsSum();
      return RpcUtils.getStatus(
          TSStatusCode.SESSION_NUMS_EXCEEDED,
          String.format(
              "dn_rpc_max_concurrent_client_num (%d) cannot be less than "
                  + "the sum of all users' MIN_SESSION_PER_USER (%d). "
                  + "Please set it to at least %d.",
              n, minSum, minSum));
    }
    return null;
  }

  public TSStatus checkSessionNumOnConnect(
      Map<String, Integer> currentSessionInfo, int rpcMaxConcurrentClientNum) {
    TSStatus status = confirmLeader();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return ((TimechoPermissionManager) permissionManager)
          .checkSessionNumOnConnect(currentSessionInfo, rpcMaxConcurrentClientNum);
    } else {
      return status;
    }
  }

  public TCheckMaxClientNumResp checkMaxClientNumValid(int maxConcurrentClientNum) {
    TSStatus status = confirmLeader();
    TCheckMaxClientNumResp resp = new TCheckMaxClientNumResp();
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      try {
        resp =
            ((TimechoPermissionManager) permissionManager)
                .checkMaxClientNumValid(maxConcurrentClientNum);
      } catch (AuthException e) {
        status.setCode(e.getCode().getStatusCode()).setMessage(e.getMessage());
        resp.setStatus(status);
        return resp;
      }
    } else {
      resp.setStatus(status);
    }
    return resp;
  }
}
