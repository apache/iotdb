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

package org.apache.iotdb.confignode.manager.externalservice;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.externalservice.ServiceInfo;
import org.apache.iotdb.confignode.consensus.request.read.exernalservice.ShowExternalServicePlan;
import org.apache.iotdb.confignode.consensus.request.write.externalservice.CreateExternalServicePlan;
import org.apache.iotdb.confignode.consensus.request.write.externalservice.DropExternalServicePlan;
import org.apache.iotdb.confignode.consensus.request.write.externalservice.StartExternalServicePlan;
import org.apache.iotdb.confignode.consensus.request.write.externalservice.StopExternalServicePlan;
import org.apache.iotdb.confignode.consensus.response.externalservice.ShowExternalServiceResp;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.rpc.thrift.TCreateExternalServiceReq;
import org.apache.iotdb.confignode.rpc.thrift.TShowExternalServiceResp;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class ExternalServiceManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExternalServiceManager.class);

  private final ConfigManager configManager;

  public ExternalServiceManager(ConfigManager configManager) {
    this.configManager = configManager;
  }

  public TSStatus createService(TCreateExternalServiceReq req) {
    try {
      return configManager
          .getConsensusManager()
          .write(
              new CreateExternalServicePlan(
                  req.getDataNodeId(),
                  new ServiceInfo(
                      req.getServiceName(),
                      req.getClassName(),
                      ServiceInfo.ServiceType.USER_DEFINED)));
    } catch (ConsensusException e) {
      LOGGER.warn(
          "Unexpected error happened while creating Service {} on DataNode {}: ",
          req.getServiceName(),
          req.getDataNodeId(),
          e);
      // consensus layer related errors
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return res;
    }
  }

  public TSStatus startService(int dataNodeId, String serviceName) {
    try {
      return configManager
          .getConsensusManager()
          .write(new StartExternalServicePlan(dataNodeId, serviceName));
    } catch (ConsensusException e) {
      LOGGER.warn(
          "Unexpected error happened while starting Service {} on DataNode {}: ",
          serviceName,
          dataNodeId,
          e);
      // consensus layer related errors
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return res;
    }
  }

  public TSStatus stopService(int dataNodeId, String serviceName) {
    try {
      return configManager
          .getConsensusManager()
          .write(new StopExternalServicePlan(dataNodeId, serviceName));
    } catch (ConsensusException e) {
      LOGGER.warn(
          "Unexpected error happened while stopping Service {} on DataNode {}: ",
          serviceName,
          dataNodeId,
          e);
      // consensus layer related errors
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return res;
    }
  }

  public TSStatus dropService(int dataNodeId, String serviceName) {
    try {
      return configManager
          .getConsensusManager()
          .write(new DropExternalServicePlan(dataNodeId, serviceName));
    } catch (ConsensusException e) {
      LOGGER.warn(
          "Unexpected error happened while dropping Service {} on DataNode {}: ",
          serviceName,
          dataNodeId,
          e);
      // consensus layer related errors
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return res;
    }
  }

  public TShowExternalServiceResp showService(int dataNodeId) {
    try {
      DataSet response =
          configManager.getConsensusManager().read(new ShowExternalServicePlan(dataNodeId));
      return ((ShowExternalServiceResp) response).convertToRpcShowExternalServiceResp();
    } catch (ConsensusException e) {
      LOGGER.warn("Unexpected error happened while showing Service: ", e);
      // consensus layer related errors
      TSStatus res = new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
      res.setMessage(e.getMessage());
      return new TShowExternalServiceResp(res, Collections.emptyList());
    }
  }
}
