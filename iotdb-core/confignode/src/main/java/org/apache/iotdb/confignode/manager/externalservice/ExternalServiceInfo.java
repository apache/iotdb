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
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.confignode.consensus.request.write.externalservice.CreateExternalServicePlan;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ExternalServiceInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExternalServiceInfo.class);

  private final Map<Integer, Map<String, ServiceInfo>> datanodeToServiceInfos;

  private static final String SNAPSHOT_FILENAME = "service_info.bin";

  public ExternalServiceInfo() {
    datanodeToServiceInfos = new ConcurrentHashMap<>();
  }

  /**
   * Add a new ExternalService only if there was no mapping for <tt>this service</tt> on target
   * DataNode, otherwise ignore this operation.
   *
   * @return SUCCESS_STATUS if there was no mapping for <tt>this service</tt> on target DataNode,
   *     otherwise EXTERNAL_SERVICE_AlREADY_EXIST
   */
  public TSStatus addService(CreateExternalServicePlan plan) {
    TSStatus res = new TSStatus();
    Map<String, ServiceInfo> serviceInfos =
        datanodeToServiceInfos.computeIfAbsent(plan.getDatanodeId(), k -> new HashMap<>());
    String serviceName = plan.getServiceInfo().getServiceName();
    if (serviceInfos.containsKey(serviceName)) {
      res.code = TSStatusCode.EXTERNAL_SERVICE_ALREADY_EXIST.getStatusCode();
      res.message =
          String.format(
              "ExternalService %s has already been created on DataNode .",
              serviceName, plan.getDatanodeId());
    } else {
      serviceInfos.put(serviceName, plan.getServiceInfo());
      res.code = TSStatusCode.SUCCESS_STATUS.getStatusCode();
    }
    return res;
  }

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws IOException {
    File snapshotFile = new File(snapshotDir, SNAPSHOT_FILENAME);
    if (snapshotFile.exists() && snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to take snapshot, because snapshot file [{}] is already exist.",
          snapshotFile.getAbsolutePath());
      return false;
    }

    try (FileOutputStream fileOutputStream = new FileOutputStream(snapshotFile)) {

      // serializeExistedJarToMD5(fileOutputStream);

      // udfTable.serializeUDFTable(fileOutputStream);

      // fsync
      fileOutputStream.getFD().sync();

      return true;
    }
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws IOException {
    File snapshotFile = new File(snapshotDir, SNAPSHOT_FILENAME);
    if (!snapshotFile.exists() || !snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to load snapshot,snapshot file [{}] is not exist.",
          snapshotFile.getAbsolutePath());
      return;
    }

    // acquireUDFTableLock();
    try (FileInputStream fileInputStream = new FileInputStream(snapshotFile)) {

      clear();

      // deserializeExistedJarToMD5(fileInputStream);

      // udfTable.deserializeUDFTable(fileInputStream);
    } finally {
      // releaseUDFTableLock();
    }
  }

  public void clear() {
    // existedJarToMD5.clear();
    // udfTable.clear();
  }
}
