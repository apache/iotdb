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
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.service.external.ServiceInformation;
import org.apache.iotdb.commons.service.external.ServiceStatus;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.client.async.CnToDnInternalServiceAsyncRequestManager;
import org.apache.iotdb.confignode.client.async.handlers.DataNodeAsyncRequestContext;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.read.service.GetServiceJarPlan;
import org.apache.iotdb.confignode.consensus.request.read.service.GetServiceTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.service.CreateServicePlan;
import org.apache.iotdb.confignode.consensus.request.write.service.DropServicePlan;
import org.apache.iotdb.confignode.consensus.request.write.service.UpdateServiceStatusPlan;
import org.apache.iotdb.confignode.consensus.response.JarResp;
import org.apache.iotdb.confignode.consensus.response.service.ServiceTableResp;
import org.apache.iotdb.confignode.persistence.ServiceInfo;
import org.apache.iotdb.confignode.rpc.thrift.TCreateServiceReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetJarInListReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetJarInListResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetServiceTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowServiceInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowServiceResp;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.mpp.rpc.thrift.TCreateServiceInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TDropServiceInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TShowServiceInstanceResp;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ServiceManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceManager.class);

  private final long planSizeLimit =
      ConfigNodeDescriptor.getInstance()
              .getConf()
              .getConfigNodeRatisConsensusLogAppenderBufferSize()
          - IoTDBConstant.RAFT_LOG_BASIC_SIZE;

  private final ConfigManager configManager;

  private final ServiceInfo serviceInfo;

  public ServiceManager(ConfigManager configManager, ServiceInfo serviceInfo) {
    this.configManager = configManager;
    this.serviceInfo = serviceInfo;
  }

  public ServiceInfo getServiceInfo() {
    return serviceInfo;
  }

  public TSStatus createService(TCreateServiceReq createServiceReq) {
    try {
      String serviceName = createServiceReq.getServiceName();
      boolean isUsingUrI = createServiceReq.isIsUsingURI();
      String jarName = createServiceReq.getJarName();
      byte[] jarFile = createServiceReq.getJarFile();

      serviceInfo.validate(serviceName, jarName, createServiceReq.getJarMD5());
      ServiceInformation serviceInformation =
          new ServiceInformation(
              createServiceReq.getServiceName(),
              createServiceReq.getClassName(),
              isUsingUrI,
              jarName,
              createServiceReq.getJarMD5(),
              ServiceStatus.INACTIVE);
      LOGGER.info("Start to add service: {} in SERVICE_Table on Config Node", serviceName);
      boolean needToSaveJar = isUsingUrI && serviceInfo.needToSaveJar(jarName);
      CreateServicePlan createServicePlan =
          new CreateServicePlan(serviceInformation, needToSaveJar ? new Binary(jarFile) : null);

      if (needToSaveJar && createServicePlan.getSerializedSize() > planSizeLimit) {
        String errorMessage =
            String.format(
                "Fail to create Service[%s], the size of Jar is too large, you can increase the value of property 'config_node_ratis_log_appender_buffer_size_max' on ConfigNode",
                serviceName);
        LOGGER.error(errorMessage);
        return new TSStatus(TSStatusCode.SERVICE_CREATE_SERVICE_ERROR.getStatusCode())
            .setMessage(errorMessage);
      }
      TSStatus preCreateStatus = configManager.getConsensusManager().write(createServicePlan);
      if (preCreateStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return preCreateStatus;
      }
      LOGGER.info("Successfully add service: {} in SERVICE_Table on Config Node", serviceName);

      LOGGER.info(
          "Start to create Service [{}] on Data Nodes, needToSaveJar[{}]",
          serviceName,
          needToSaveJar);
      TSStatus dataNodesStatus =
          RpcUtils.squashResponseStatusList(
              createServiceOnDataNodes(serviceInformation, needToSaveJar ? jarFile : null));
      if (dataNodesStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return dataNodesStatus;
      }
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())
          .setMessage(
              String.format(
                  "Successfully create Service [%s] on Config Node and Data Nodes", serviceName));
    } catch (Exception e) {
      LOGGER.warn(e.getMessage(), e);
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage(e.getMessage());
    }
  }

  private List<TSStatus> createServiceOnDataNodes(
      ServiceInformation serviceInformation, byte[] jarFile) throws IOException {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    TCreateServiceInstanceReq createServiceInstanceReq = new TCreateServiceInstanceReq();
    createServiceInstanceReq.setServiceInformation(serviceInformation.serialize());
    createServiceInstanceReq.setJarFile(jarFile);
    DataNodeAsyncRequestContext<TCreateServiceInstanceReq, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.CREATE_SERVICE, createServiceInstanceReq, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
    return clientHandler.getResponseList();
  }

  public TSStatus dropService(String serviceName) {
    try {
      ServiceInformation serviceInformation = serviceInfo.getServiceInformation(serviceName);
      if (serviceInformation == null) {
        return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
            .setMessage(String.format("Service [%s] not found", serviceName));
      }
      LOGGER.info("Start to drop Service [{}] on Data Nodes", serviceName);
      TSStatus dataNodesStatus =
          RpcUtils.squashResponseStatusList(dropServiceOnDataNodes(serviceName));
      if (dataNodesStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return dataNodesStatus;
      }
      LOGGER.info("Successfully drop service: {}  on Data Node", serviceName);

      LOGGER.info("Start to drop service: {} in SERVICE_Table on Config Node", serviceName);
      TSStatus status = configManager.getConsensusManager().write(new DropServicePlan(serviceName));
      if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.info("Failed to drop service: {} in SERVICE_Table on Config Node", serviceName);
      } else {
        LOGGER.info("Successfully drop service: {} in SERVICE_Table on Config Node", serviceName);
      }

      return status;
    } catch (ConsensusException e) {
      LOGGER.error(e.getMessage(), e);
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage(e.getMessage());
    }
  }

  private List<TSStatus> dropServiceOnDataNodes(String serviceName) {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    TDropServiceInstanceReq dropServiceInstanceReq =
        new TDropServiceInstanceReq(serviceName, false);
    DataNodeAsyncRequestContext<TDropServiceInstanceReq, TSStatus> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.DROP_SERVICE, dropServiceInstanceReq, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
    return clientHandler.getResponseList();
  }

  public TSStatus updateServiceState(String serviceName, ServiceStatus serviceStatus) {
    try {
      LOGGER.info(
          "Start to update state of service [{}] in SERVICE_Table on Config Node", serviceName);
      TSStatus preUpdateStatus =
          configManager
              .getConsensusManager()
              .write(new UpdateServiceStatusPlan(serviceName, serviceStatus));
      if (preUpdateStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return preUpdateStatus;
      }
      LOGGER.info(
          "Successfully update state of service [{}] in SERVICE_Table on Config Node", serviceName);

      LOGGER.info("Start to update state of service [{}] on Data Nodes", serviceName);
      TSStatus dataNodesStatus =
          RpcUtils.squashResponseStatusList(
              updateServiceStateOnDataNodes(serviceName, serviceStatus));
      if (dataNodesStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.error("Failed to update state of service [{}] on all Data Nodes", serviceName);
      }
      return dataNodesStatus;
    } catch (ConsensusException e) {
      LOGGER.warn(e.getMessage(), e);
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage(e.getMessage());
    }
  }

  public List<TSStatus> updateServiceStateOnDataNodes(
      String serviceName, ServiceStatus serviceStatus) {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    DataNodeAsyncRequestContext<String, TSStatus> clientHandler;
    if (serviceStatus == ServiceStatus.ACTIVE) {
      LOGGER.info("Start to activate service [{}] on Data Nodes", serviceName);
      clientHandler =
          new DataNodeAsyncRequestContext<>(
              CnToDnAsyncRequestType.ACTIVE_SERVICE, serviceName, dataNodeLocationMap);
    } else {
      LOGGER.info("Start to deactivate service [{}] on Data Nodes", serviceName);
      clientHandler =
          new DataNodeAsyncRequestContext<>(
              CnToDnAsyncRequestType.INACTIVE_SERVICE, serviceName, dataNodeLocationMap);
    }

    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
    return clientHandler.getResponseList();
  }

  public TShowServiceResp showService(String serviceName) {
    ServiceInformation serviceInformation = serviceInfo.getServiceInformation(serviceName);
    if (serviceInformation == null) {
      return new TShowServiceResp(
          new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
              .setMessage(String.format("Service [%s] not found", serviceName)));
    }
    Map<Integer, TShowServiceInstanceResp> serviceResponses = showServiceOnDataNodes(serviceName);
    String className = serviceInformation.getClassName();
    TShowServiceResp resp =
        new TShowServiceResp(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    List<TShowServiceInfo> serviceInfoList = new ArrayList<>();
    for (Map.Entry<Integer, TShowServiceInstanceResp> serviceResponse :
        serviceResponses.entrySet()) {

      TShowServiceInstanceResp instanceResp = serviceResponse.getValue();
      TSStatus tsStatus = instanceResp.getStatus();
      String state = "";
      if (instanceResp.getServiceState() != null) {
        state = instanceResp.getServiceState();
      }
      String message = "";
      if (tsStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        message = tsStatus.getMessage();
      }
      TShowServiceInfo serviceInfo =
          new TShowServiceInfo(
              serviceName, className, String.valueOf(serviceResponse.getKey()), state);
      serviceInfo.setMessage(message);
      serviceInfoList.add(serviceInfo);
    }
    resp.setServiceInfoList(serviceInfoList);
    return resp;
  }

  private Map<Integer, TShowServiceInstanceResp> showServiceOnDataNodes(String serviceName) {
    final Map<Integer, TDataNodeLocation> dataNodeLocationMap =
        configManager.getNodeManager().getRegisteredDataNodeLocations();
    DataNodeAsyncRequestContext<String, TShowServiceInstanceResp> clientHandler =
        new DataNodeAsyncRequestContext<>(
            CnToDnAsyncRequestType.SHOW_SERVICE, serviceName, dataNodeLocationMap);
    CnToDnInternalServiceAsyncRequestManager.getInstance().sendAsyncRequestWithRetry(clientHandler);
    return clientHandler.getResponseMap();
  }

  public TShowServiceResp showAllServices() {
    TShowServiceResp resp = new TShowServiceResp();
    resp.setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));

    List<TShowServiceInfo> serviceInfoList = new ArrayList<>();
    for (ServiceInformation serviceInfo : serviceInfo.getAllServiceInformation()) {
      TShowServiceInfo serviceInfoItem = new TShowServiceInfo();
      serviceInfoItem.setServiceName(serviceInfo.getServiceName());
      serviceInfoItem.setClassName(serviceInfo.getClassName());
      serviceInfoList.add(serviceInfoItem);
    }
    resp.setServiceInfoList(serviceInfoList);
    return resp;
  }

  public TGetServiceTableResp getServiceTable() {
    try {
      return ((ServiceTableResp)
              configManager.getConsensusManager().read(new GetServiceTablePlan()))
          .convertToThriftResponse();
    } catch (ConsensusException | IOException e) {
      LOGGER.error("failed to get service table", e);
      return new TGetServiceTableResp(
          new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
              .setMessage(e.getMessage()),
          Collections.emptyList());
    }
  }

  public TGetJarInListResp getServiceJarList(TGetJarInListReq req) {
    try {
      return ((JarResp)
              configManager.getConsensusManager().read(new GetServiceJarPlan(req.getJarNameList())))
          .convertToThriftResponse();
    } catch (ConsensusException e) {
      LOGGER.warn("Failed in the read API executing the consensus layer due to: ", e);
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return new JarResp(res, Collections.emptyList()).convertToThriftResponse();
    }
  }
}
