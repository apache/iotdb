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

package org.apache.iotdb.confignode.persistence;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.executable.ExecutableResource;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.commons.udf.service.UDFClassLoader;
import org.apache.iotdb.commons.udf.service.UDFExecutableManager;
import org.apache.iotdb.commons.udf.service.UDFRegistrationService;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.write.CreateFunctionPlan;
import org.apache.iotdb.confignode.consensus.request.write.DropFunctionPlan;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class UDFInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(UDFInfo.class);

  private static final ConfigNodeConfig CONFIG_NODE_CONF =
      ConfigNodeDescriptor.getInstance().getConf();

  private final UDFExecutableManager udfExecutableManager;
  private final UDFRegistrationService udfRegistrationService;

  public UDFInfo() {
    udfExecutableManager =
        UDFExecutableManager.setupAndGetInstance(
            CONFIG_NODE_CONF.getTemporaryLibDir(), CONFIG_NODE_CONF.getUdfLibDir());
    udfRegistrationService =
        UDFRegistrationService.setupAndGetInstance(CONFIG_NODE_CONF.getSystemUdfDir());
  }

  public synchronized void validateBeforeRegistration(
      String functionName, String className, List<String> uris) throws Exception {
    udfRegistrationService.validate(functionName, className);

    if (uris.isEmpty()) {
      fetchExecutablesAndCheckInstantiation(className);
    } else {
      fetchExecutablesAndCheckInstantiation(className, uris);
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
    final ExecutableResource resource = udfExecutableManager.request(uris);
    try (UDFClassLoader temporaryUdfClassLoader = new UDFClassLoader(resource.getResourceDir())) {
      Class.forName(className, true, temporaryUdfClassLoader)
          .getDeclaredConstructor()
          .newInstance();
    } finally {
      udfExecutableManager.removeFromTemporaryLibRoot(resource);
    }
  }

  public synchronized TSStatus createFunction(CreateFunctionPlan req) {
    final String functionName = req.getFunctionName();
    final String className = req.getClassName();
    final List<String> uris = req.getUris();

    try {
      udfRegistrationService.register(functionName, className, uris, udfExecutableManager, true);
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (Exception e) {
      final String errorMessage =
          String.format(
              "[ConfigNode] Failed to register UDF %s(class name: %s, uris: %s), because of exception: %s",
              functionName, className, uris, e);
      LOGGER.warn(errorMessage, e);
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage(errorMessage);
    }
  }

  public synchronized TSStatus dropFunction(DropFunctionPlan req) {
    try {
      udfRegistrationService.deregister(req.getFunctionName());
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (Exception e) {
      final String errorMessage =
          String.format(
              "[ConfigNode] Failed to deregister UDF %s, because of exception: %s",
              req.getFunctionName(), e);
      LOGGER.warn(errorMessage, e);
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage(errorMessage);
    }
  }

  @Override
  public synchronized boolean processTakeSnapshot(File snapshotDir) throws IOException {
    return udfExecutableManager.processTakeSnapshot(snapshotDir)
        && udfRegistrationService.processTakeSnapshot(snapshotDir);
  }

  @Override
  public synchronized void processLoadSnapshot(File snapshotDir) throws IOException {
    udfExecutableManager.processLoadSnapshot(snapshotDir);
    udfRegistrationService.processLoadSnapshot(snapshotDir);
  }
}
