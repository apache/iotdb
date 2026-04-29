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

import org.apache.iotdb.common.rpc.thrift.TExternalServiceEntry;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.externalservice.ServiceInfo;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.consensus.request.write.externalservice.CreateExternalServicePlan;
import org.apache.iotdb.confignode.consensus.request.write.externalservice.DropExternalServicePlan;
import org.apache.iotdb.confignode.consensus.request.write.externalservice.StartExternalServicePlan;
import org.apache.iotdb.confignode.consensus.request.write.externalservice.StopExternalServicePlan;
import org.apache.iotdb.confignode.consensus.response.externalservice.ShowExternalServiceResp;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.zip.CRC32;

public class ExternalServiceInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExternalServiceInfo.class);

  private final Map<Integer, Map<String, ServiceInfo>> datanodeToServiceInfos;

  private static final String SNAPSHOT_FILENAME = "service_info.bin";
  private static final byte SERIALIZATION_VERSION = 1;
  private final CRC32 crc32 = new CRC32();

  private static final String SERVICE_NOT_EXISTED =
      "ExternalService %s is not existed on DataNode %s.";

  public ExternalServiceInfo() {
    datanodeToServiceInfos = new ConcurrentHashMap<>();
  }

  /**
   * Add a new ExternalService on target DataNode.
   *
   * @return SUCCESS_STATUS if <tt>this service</tt> was not existed on target DataNode, otherwise
   *     EXTERNAL_SERVICE_AlREADY_EXIST
   */
  public TSStatus addService(CreateExternalServicePlan plan) {
    TSStatus res = new TSStatus();
    Map<String, ServiceInfo> serviceInfos =
        datanodeToServiceInfos.computeIfAbsent(
            plan.getDatanodeId(), k -> new ConcurrentHashMap<>());
    String serviceName = plan.getServiceInfo().getServiceName();
    if (serviceInfos.containsKey(serviceName)) {
      res.code = TSStatusCode.EXTERNAL_SERVICE_ALREADY_EXIST.getStatusCode();
      res.message =
          String.format(
              "ExternalService %s has already been created on DataNode %s.",
              serviceName, plan.getDatanodeId());
    } else {
      serviceInfos.put(serviceName, plan.getServiceInfo());
      res.code = TSStatusCode.SUCCESS_STATUS.getStatusCode();
    }
    return res;
  }

  /**
   * Drop the ExternalService whose name is same as <tt>serviceName</tt> in plan.
   *
   * @return SUCCESS_STATUS if <tt>this service</tt> was existed on target DataNode, otherwise
   *     NO_SUCH_EXTERNAL_SERVICE
   */
  public TSStatus dropService(DropExternalServicePlan plan) {
    TSStatus res = new TSStatus();
    Map<String, ServiceInfo> serviceInfos =
        datanodeToServiceInfos.computeIfAbsent(
            plan.getDataNodeId(), k -> new ConcurrentHashMap<>());
    String serviceName = plan.getServiceName();
    ServiceInfo removed = serviceInfos.remove(serviceName);
    if (removed == null) {
      res.code = TSStatusCode.NO_SUCH_EXTERNAL_SERVICE.getStatusCode();
      res.message = String.format(SERVICE_NOT_EXISTED, serviceName, plan.getDataNodeId());
    } else {
      res.code = TSStatusCode.SUCCESS_STATUS.getStatusCode();
    }
    return res;
  }

  /**
   * Start the ExternalService whose name is same as <tt>serviceName</tt> in plan.
   *
   * @return SUCCESS_STATUS if <tt>this service</tt> was existed on target DataNode, otherwise
   *     NO_SUCH_EXTERNAL_SERVICE
   */
  public TSStatus startService(StartExternalServicePlan plan) {
    TSStatus res = new TSStatus();
    Map<String, ServiceInfo> serviceInfos =
        datanodeToServiceInfos.computeIfAbsent(
            plan.getDataNodeId(), k -> new ConcurrentHashMap<>());
    String serviceName = plan.getServiceName();
    ServiceInfo serviceInfo = serviceInfos.get(serviceName);
    if (serviceInfo == null) {
      res.code = TSStatusCode.NO_SUCH_EXTERNAL_SERVICE.getStatusCode();
      res.message = String.format(SERVICE_NOT_EXISTED, serviceName, plan.getDataNodeId());
    } else {
      serviceInfo.setState(ServiceInfo.State.RUNNING);
      res.code = TSStatusCode.SUCCESS_STATUS.getStatusCode();
    }
    return res;
  }

  /**
   * Stop the ExternalService whose name is same as <tt>serviceName</tt> in plan.
   *
   * @return SUCCESS_STATUS if <tt>this service</tt> was existed on target DataNode, otherwise
   *     NO_SUCH_EXTERNAL_SERVICE
   */
  public TSStatus stopService(StopExternalServicePlan plan) {
    TSStatus res = new TSStatus();
    Map<String, ServiceInfo> serviceInfos =
        datanodeToServiceInfos.computeIfAbsent(
            plan.getDataNodeId(), k -> new ConcurrentHashMap<>());
    String serviceName = plan.getServiceName();
    ServiceInfo serviceInfo = serviceInfos.get(serviceName);
    if (serviceInfo == null) {
      res.code = TSStatusCode.NO_SUCH_EXTERNAL_SERVICE.getStatusCode();
      res.message = String.format(SERVICE_NOT_EXISTED, serviceName, plan.getDataNodeId());
    } else {
      serviceInfo.setState(ServiceInfo.State.STOPPED);
      res.code = TSStatusCode.SUCCESS_STATUS.getStatusCode();
    }
    return res;
  }

  public ShowExternalServiceResp showService(Set<Integer> dataNodes) {
    return new ShowExternalServiceResp(
        datanodeToServiceInfos.entrySet().stream()
            .filter(entry -> dataNodes.contains(entry.getKey()))
            .flatMap(
                entry ->
                    entry.getValue().values().stream()
                        .map(
                            serviceInfo ->
                                new TExternalServiceEntry(
                                    serviceInfo.getServiceName(),
                                    serviceInfo.getClassName(),
                                    serviceInfo.getState().getValue(),
                                    entry.getKey(),
                                    ServiceInfo.ServiceType.USER_DEFINED.getValue())))
            .collect(Collectors.toList()));
  }

  private void serializeInfos(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(SERIALIZATION_VERSION, outputStream);
    ReadWriteIOUtils.write(datanodeToServiceInfos.size(), outputStream);
    for (Map.Entry<Integer, Map<String, ServiceInfo>> outerEntry :
        datanodeToServiceInfos.entrySet()) {
      ReadWriteIOUtils.write(outerEntry.getKey(), outputStream); // DataNode ID

      Map<String, ServiceInfo> innerMap = outerEntry.getValue();
      // inner Map
      ReadWriteIOUtils.write(innerMap.size(), outputStream);
      for (ServiceInfo innerEntry : innerMap.values()) {
        serializeServiceInfoWithCRC(innerEntry, outputStream);
      }
    }
  }

  private void serializeServiceInfoWithCRC(ServiceInfo serviceInfo, OutputStream outputStream)
      throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream tempDos = new DataOutputStream(byteArrayOutputStream);
    serviceInfo.serialize(tempDos);
    tempDos.flush();
    byte[] bytes = byteArrayOutputStream.toByteArray();

    crc32.reset();
    crc32.update(bytes, 0, bytes.length);

    ReadWriteIOUtils.write(bytes.length, outputStream);
    outputStream.write(bytes);
    ReadWriteIOUtils.write((int) crc32.getValue(), outputStream);
  }

  private void deserializeInfos(InputStream inputStream) throws IOException {
    if (ReadWriteIOUtils.readByte(inputStream) != SERIALIZATION_VERSION) {
      throw new IOException("Incorrect version of " + SNAPSHOT_FILENAME);
    }

    int outerSize = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < outerSize; i++) {
      int dataNodeId = ReadWriteIOUtils.readInt(inputStream);
      int innerSize = ReadWriteIOUtils.readInt(inputStream);

      Map<String, ServiceInfo> innerMap =
          datanodeToServiceInfos.computeIfAbsent(
              dataNodeId, k -> new ConcurrentHashMap<>(innerSize));
      for (int j = 0; j < innerSize; j++) {
        ServiceInfo value = deserializeServiceInfoConsiderCRC(inputStream);
        if (value != null) {
          innerMap.put(value.getServiceName(), value);
        }
      }
      datanodeToServiceInfos.put(dataNodeId, innerMap);
    }
  }

  private ServiceInfo deserializeServiceInfoConsiderCRC(InputStream inputStream)
      throws IOException {
    int length = ReadWriteIOUtils.readInt(inputStream);
    byte[] bytes = new byte[length];
    inputStream.read(bytes);

    crc32.reset();
    crc32.update(bytes, 0, length);

    int expectedCRC = ReadWriteIOUtils.readInt(inputStream);
    if ((int) crc32.getValue() != expectedCRC) {
      LOGGER.error("Mismatched CRC32 code when deserializing service info.");
      return null;
    }

    return ServiceInfo.deserialize(ByteBuffer.wrap(bytes));
  }

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws IOException {
    // do nothing if there is no any user-defined ServiceInfo
    if (datanodeToServiceInfos.isEmpty()) {
      return true;
    }

    File snapshotFile = new File(snapshotDir, SNAPSHOT_FILENAME);
    if (snapshotFile.exists() && snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to take snapshot, because snapshot file [{}] is already exist.",
          snapshotFile.getAbsolutePath());
      return false;
    }

    try (FileOutputStream fileOutputStream = new FileOutputStream(snapshotFile)) {

      serializeInfos(fileOutputStream);

      // fsync
      fileOutputStream.getFD().sync();

      return true;
    }
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws IOException {
    File snapshotFile = new File(snapshotDir, SNAPSHOT_FILENAME);

    if (!snapshotFile.exists()) {
      // do nothing if the snapshot file is not existed
      return;
    }

    if (!snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to load snapshot,snapshot file [{}] is not a normal file.",
          snapshotFile.getAbsolutePath());
      return;
    }

    try (FileInputStream fileInputStream = new FileInputStream(snapshotFile)) {

      clear();

      deserializeInfos(fileInputStream);
    }
  }

  public void clear() {
    datanodeToServiceInfos.clear();
  }

  @TestOnly
  public Map<Integer, Map<String, ServiceInfo>> getRawDatanodeToServiceInfos() {
    return datanodeToServiceInfos;
  }
}
