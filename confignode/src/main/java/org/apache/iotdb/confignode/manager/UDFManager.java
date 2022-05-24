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

import org.apache.iotdb.common.rpc.thrift.TDataNodeInfo;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.udf.service.UDFClassLoader;
import org.apache.iotdb.commons.udf.service.UDFExecutableManager;
import org.apache.iotdb.commons.udf.service.UDFExecutableResource;
import org.apache.iotdb.confignode.client.AsyncDataNodeClientPool;
import org.apache.iotdb.confignode.client.handlers.CreateFunctionHandler;
import org.apache.iotdb.confignode.conf.ConfigNodeConf;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.write.CreateFunctionReq;
import org.apache.iotdb.confignode.persistence.UDFInfo;
import org.apache.iotdb.mpp.rpc.thrift.TCreateFunctionRequest;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

public class UDFManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(UDFManager.class);

  private static final ConfigNodeConf CONFIG_NODE_CONF =
      ConfigNodeDescriptor.getInstance().getConf();

  private final ConfigManager configManager;
  private final UDFInfo udfInfo;
  private final UDFExecutableManager udfExecutableManager;

  public UDFManager(ConfigManager configManager, UDFInfo udfInfo) throws IOException {
    this.configManager = configManager;
    this.udfInfo = udfInfo;
    udfExecutableManager =
        UDFExecutableManager.setupAndGetInstance(
            CONFIG_NODE_CONF.getTemporaryLibDir(), CONFIG_NODE_CONF.getUdfLibDir());
  }

  // TODO: using procedure
  public TSStatus createFunction(String functionName, String className, List<String> uris) {
    try {
      if (uris.isEmpty()) {
        fetchExecutablesAndCheckInstantiation(className);
      } else {
        fetchExecutablesAndCheckInstantiation(className, uris);
      }

      final TSStatus configNodeStatus =
          configManager
              .getConsensusManager()
              .write(new CreateFunctionReq(functionName, className, uris))
              .getStatus();
      if (configNodeStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return configNodeStatus;
      }

      return squashDataNodeResponseStatusList(
          createFunctionOnDataNodes(functionName, className, uris));
    } catch (Exception e) {
      final String errorMessage =
          String.format(
              "Failed to register UDF %s(class name: %s, uris: %s), because of exception: %s",
              functionName, className, uris, e);
      LOGGER.warn(errorMessage);
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage(errorMessage);
    }
  }

  private void fetchExecutablesAndCheckInstantiation(String className) throws Exception {
    try (UDFClassLoader temporaryUdfClassLoader =
        new UDFClassLoader(CONFIG_NODE_CONF.getUdfLibDir())) {
      Class.forName(className, true, temporaryUdfClassLoader)
          .getDeclaredConstructor()
          .newInstance();
    }
  }

  private void fetchExecutablesAndCheckInstantiation(String className, List<String> uris)
      throws Exception {
    final UDFExecutableResource resource = udfExecutableManager.request(uris);
    try (UDFClassLoader temporaryUdfClassLoader = new UDFClassLoader(resource.getResourceDir())) {
      Class.forName(className, true, temporaryUdfClassLoader)
          .getDeclaredConstructor()
          .newInstance();
    } finally {
      udfExecutableManager.removeFromTemporaryLibRoot(resource);
    }
  }

  private List<TSStatus> createFunctionOnDataNodes(
      String functionName, String className, List<String> uris) {
    final List<TDataNodeInfo> onlineDataNodes =
        configManager.getNodeManager().getOnlineDataNodes(-1);
    final List<TSStatus> dataNodeResponseStatus =
        Collections.synchronizedList(new ArrayList<>(onlineDataNodes.size()));
    final CountDownLatch countDownLatch = new CountDownLatch(onlineDataNodes.size());
    final TCreateFunctionRequest request =
        new TCreateFunctionRequest(functionName, className, uris);

    for (TDataNodeInfo dataNodeInfo : onlineDataNodes) {
      final TEndPoint endPoint = dataNodeInfo.getLocation().getInternalEndPoint();
      AsyncDataNodeClientPool.getInstance()
          .createFunction(
              endPoint,
              request,
              new CreateFunctionHandler(
                  countDownLatch, dataNodeResponseStatus, endPoint.getIp(), endPoint.getPort()));
    }

    try {
      countDownLatch.await();
    } catch (InterruptedException e) {
      LOGGER.error("UDFManager was interrupted during creating functions on data nodes", e);
    }

    return dataNodeResponseStatus;
  }

  private TSStatus squashDataNodeResponseStatusList(List<TSStatus> dataNodeResponseStatusList) {
    final List<TSStatus> failedStatus =
        dataNodeResponseStatusList.stream()
            .filter(status -> status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode())
            .collect(Collectors.toList());
    return failedStatus.isEmpty()
        ? new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())
        : new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
            .setMessage(failedStatus.toString());
  }
}
