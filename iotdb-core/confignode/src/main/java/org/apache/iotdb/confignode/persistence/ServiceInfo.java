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
import org.apache.iotdb.commons.executable.ExecutableManager;
import org.apache.iotdb.commons.service.external.ServiceExecutableManager;
import org.apache.iotdb.commons.service.external.ServiceInformation;
import org.apache.iotdb.commons.service.external.ServiceTable;
import org.apache.iotdb.commons.service.external.exception.ServiceManagementException;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.read.service.GetServiceJarPlan;
import org.apache.iotdb.confignode.consensus.request.write.service.CreateServicePlan;
import org.apache.iotdb.confignode.consensus.request.write.service.UpdateServiceStatusPlan;
import org.apache.iotdb.confignode.consensus.response.JarResp;
import org.apache.iotdb.confignode.consensus.response.service.ServiceTableResp;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class ServiceInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceInfo.class);

  private static final ConfigNodeConfig CONFIG_NODE_CONF =
      ConfigNodeDescriptor.getInstance().getConf();

  private final ServiceTable serviceTable;
  private final Map<String, String> existedJarToMD5;

  private final ServiceExecutableManager serviceExecutableManager;

  private final ReentrantLock serviceTableLock = new ReentrantLock();

  private static final String SNAPSHOT_FILENAME = "service_info.bin";

  public ServiceInfo() throws IOException {
    this.serviceTable = new ServiceTable();
    this.existedJarToMD5 = new HashMap<>();
    this.serviceExecutableManager =
        ServiceExecutableManager.setupAndGetInstance(
            CONFIG_NODE_CONF.getServiceTemporaryLibDir(), CONFIG_NODE_CONF.getServiceDir());
  }

  private void acquireServiceTableLock() {
    LOGGER.info("acquire ServiceTableLock");
    serviceTableLock.lock();
  }

  private void releaseServiceTableLock() {
    LOGGER.info("release ServiceTableLock");
    serviceTableLock.unlock();
  }

  /** Validate whether the service can be created. */
  public void validate(String serviceName, String jarName, String jarMD5)
      throws ServiceManagementException {
    if (serviceTable.containsService(serviceName)) {
      throw new ServiceManagementException(
          String.format(
              "Failed to create service [%s], the same name service has been created",
              serviceName));
    }

    if (existedJarToMD5.containsKey(jarName) && !existedJarToMD5.get(jarName).equals(jarMD5)) {
      throw new ServiceManagementException(
          String.format(
              "Failed to create service [%s], the same name Jar [%s] but different MD5 [%s] has existed",
              serviceName, jarName, jarMD5));
    }
  }

  public TSStatus addServiceInTable(CreateServicePlan createServicePlan) {
    acquireServiceTableLock();
    try {
      String serviceName = createServicePlan.getServiceInformation().getServiceName();
      ServiceInformation serviceInformation = createServicePlan.getServiceInformation();
      serviceTable.addServiceInformation(serviceName, serviceInformation);
      if (serviceInformation.isUsingURI()) {
        existedJarToMD5.put(serviceInformation.getJarName(), serviceInformation.getJarFileMD5());
        if (createServicePlan.getJarFile() != null) {
          serviceExecutableManager.saveToInstallDir(
              ByteBuffer.wrap(createServicePlan.getJarFile().getValues()),
              serviceInformation.getJarName());
        }
      }
    } catch (IOException e) {
      final String errorMessage =
          String.format(
              "Failed to add service [%s] in ServiceTable on Config Nodes, because of %s",
              createServicePlan.getServiceInformation().getServiceName(), e);
      LOGGER.warn(errorMessage, e);
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage(errorMessage);
    } finally {
      releaseServiceTableLock();
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public TSStatus deleteServiceInTable(String serviceName) {
    acquireServiceTableLock();
    try {
      ServiceInformation serviceInformation = serviceTable.removeServiceInformation(serviceName);
      if (serviceInformation != null) {
        existedJarToMD5.remove(serviceInformation.getJarName());
      }
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } finally {
      releaseServiceTableLock();
    }
  }

  public TSStatus updateServiceState(UpdateServiceStatusPlan updateServiceStatusPlan) {
    acquireServiceTableLock();
    try {
      String serviceName = updateServiceStatusPlan.getServiceName();
      ServiceInformation serviceInformation = serviceTable.getServiceInformation(serviceName);
      if (serviceInformation == null) {
        return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
            .setMessage(String.format("Service [%s] not found", serviceName));
      }
      serviceInformation.setServiceStatus(updateServiceStatusPlan.getServiceStatus());
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (Exception e) {
      final String errorMessage =
          String.format(
              "Failed to update state of service [%s] in ServiceTable on Config Nodes, because of %s",
              updateServiceStatusPlan.getServiceName(), e);
      LOGGER.warn(errorMessage, e);
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage(errorMessage);
    } finally {
      releaseServiceTableLock();
    }
  }

  /** Validate whether the service can be dropped. */
  public void validate(String serviceName) throws ServiceManagementException {
    if (serviceTable.containsService(serviceName)) {
      return;
    }
    throw new ServiceManagementException(
        String.format(
            "Failed to drop service [%s], this Service has not been created", serviceName));
  }

  public boolean needToSaveJar(String jarName) {
    return !existedJarToMD5.containsKey(jarName);
  }

  public ServiceInformation getServiceInformation(String serviceName) {
    acquireServiceTableLock();
    try {
      if (serviceTable.getServiceInformation(serviceName) != null) {
        return serviceTable.getServiceInformation(serviceName).copy();
      }
      return null;
    } finally {
      releaseServiceTableLock();
    }
  }

  public List<ServiceInformation> getAllServiceInformation() {
    acquireServiceTableLock();
    try {
      return serviceTable.getAllServiceInformation();
    } finally {
      releaseServiceTableLock();
    }
  }

  public void serializeExistedJarToMD5(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(existedJarToMD5.size(), outputStream);
    for (Map.Entry<String, String> entry : existedJarToMD5.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      ReadWriteIOUtils.write(entry.getValue(), outputStream);
    }
  }

  public void deserializeExistedJarToMD5(InputStream inputStream) throws IOException {
    int size = ReadWriteIOUtils.readInt(inputStream);
    while (size > 0) {
      existedJarToMD5.put(
          ReadWriteIOUtils.readString(inputStream), ReadWriteIOUtils.readString(inputStream));
      size--;
    }
  }

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws TException, IOException {
    File snapshotFile = new File(snapshotDir, SNAPSHOT_FILENAME);
    if (snapshotFile.exists() && snapshotFile.isFile()) {
      LOGGER.info(
          "Snapshot file [{}] already exists, skip take snapshot", snapshotFile.getAbsolutePath());
      return false;
    }
    acquireServiceTableLock();
    try (FileOutputStream fileOutputStream = new FileOutputStream(snapshotFile)) {
      serializeExistedJarToMD5(fileOutputStream);

      serviceTable.serialize(fileOutputStream);

      // fsync
      fileOutputStream.getFD().sync();
      LOGGER.info("Take snapshot successfully, file: {}", snapshotFile.getAbsolutePath());
      return true;
    } finally {
      releaseServiceTableLock();
    }
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws TException, IOException {
    File snapshotFile = new File(snapshotDir, SNAPSHOT_FILENAME);
    if (!snapshotFile.exists() || !snapshotFile.isFile()) {
      LOGGER.warn(
          "Snapshot file [{}] does not exist, skip load snapshot", snapshotFile.getAbsolutePath());
      return;
    }
    acquireServiceTableLock();
    try (FileInputStream inputStream = new FileInputStream(snapshotFile)) {
      clear();

      deserializeExistedJarToMD5(inputStream);

      serviceTable.deserialize(inputStream);
      LOGGER.info("Load snapshot successfully, file: {}", snapshotFile.getAbsolutePath());
    } finally {
      releaseServiceTableLock();
    }
  }

  public ServiceTableResp getServiceTable() {
    return new ServiceTableResp(
        new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()),
        serviceTable.getAllServiceInformation());
  }

  public JarResp getServiceJar(GetServiceJarPlan physicalPlan) {
    List<ByteBuffer> jarList = new ArrayList<>();
    for (String jarName : physicalPlan.getJarNames()) {
      try {
        jarList.add(
            ExecutableManager.transferToBytebuffer(
                ServiceExecutableManager.getInstance().getFileStringUnderInstallByName(jarName)));
      } catch (Exception e) {
        final String errorMessage =
            String.format("Failed to get Servie Jar [%s] because of %s", jarName, e);
        LOGGER.error(errorMessage, e);
        return new JarResp(
            new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
                .setMessage("Failed to get service jar because" + errorMessage),
            Collections.emptyList());
      }
    }
    return new JarResp(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()), jarList);
  }

  public void clear() {
    existedJarToMD5.clear();
    serviceTable.clear();
  }

  @TestOnly
  public Map<String, String> getRawExistedJarToMD5() {
    return existedJarToMD5;
  }

  @TestOnly
  public Map<String, ServiceInformation> getRawServiceTable() {
    return serviceTable.getServiceTable();
  }
}
