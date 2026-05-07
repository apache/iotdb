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

import org.apache.iotdb.common.rpc.thrift.Model;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.commons.executable.ExecutableManager;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.commons.udf.UDFInformation;
import org.apache.iotdb.commons.udf.UDFTable;
import org.apache.iotdb.commons.udf.service.UDFExecutableManager;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.read.function.GetFunctionTablePlan;
import org.apache.iotdb.confignode.consensus.request.read.function.GetUDFJarPlan;
import org.apache.iotdb.confignode.consensus.request.write.function.CreateFunctionPlan;
import org.apache.iotdb.confignode.consensus.request.write.function.UpdateFunctionPlan;
import org.apache.iotdb.confignode.consensus.response.JarResp;
import org.apache.iotdb.confignode.consensus.response.function.FunctionTableResp;
import org.apache.iotdb.confignode.i18n.ConfigNodeMessages;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.udf.api.exception.UDFManagementException;

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

public class UDFInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(UDFInfo.class);

  private static final ConfigNodeConfig CONFIG_NODE_CONF =
      ConfigNodeDescriptor.getInstance().getConf();

  private final UDFTable udfTable;
  private final Map<String, String> existedJarToMD5;

  private final UDFExecutableManager udfExecutableManager;

  private final ReentrantLock udfTableLock = new ReentrantLock();

  private static final String SNAPSHOT_FILENAME = "udf_info.bin";

  public UDFInfo() throws IOException {
    udfTable = new UDFTable();
    existedJarToMD5 = new HashMap<>();
    udfExecutableManager =
        UDFExecutableManager.setupAndGetInstance(
            CONFIG_NODE_CONF.getUdfTemporaryLibDir(), CONFIG_NODE_CONF.getUdfDir());
  }

  public void acquireUDFTableLock() {
    LOGGER.info(ConfigNodeMessages.ACQUIRE_UDFTABLELOCK);
    udfTableLock.lock();
  }

  public void releaseUDFTableLock() {
    LOGGER.info(ConfigNodeMessages.RELEASE_UDFTABLELOCK);
    udfTableLock.unlock();
  }

  /** Validate whether the UDF can be created. */
  public void validate(Model model, String udfName, String jarName, String jarMD5)
      throws UDFManagementException {
    if (udfTable.containsUDF(model, udfName)
        && udfTable.getUDFInformation(model, udfName).isAvailable()) {
      throw new IoTDBRuntimeException(
          String.format(
              ConfigNodeMessages.FAILED_TO_CREATE_UDF_THE_SAME_NAME_UDF_HAS_BEEN, udfName),
          TSStatusCode.UDF_ALREADY_EXISTS.getStatusCode());
    }

    if (existedJarToMD5.containsKey(jarName) && !existedJarToMD5.get(jarName).equals(jarMD5)) {
      throw new IoTDBRuntimeException(
          String.format(
              ConfigNodeMessages.FAILED_TO_CREATE_UDF_THE_SAME_NAME_JAR_BUT_DIFFERENT,
              udfName,
              jarName,
              jarMD5),
          TSStatusCode.UDF_ALREADY_EXISTS.getStatusCode());
    }
  }

  /** Validate whether the UDF can be dropped. */
  public UDFInformation getUDFInformation(Model model, String udfName)
      throws UDFManagementException {
    if (udfTable.containsUDF(model, udfName)) {
      return udfTable.getUDFInformation(model, udfName);
    }
    throw new UDFManagementException(
        String.format(
            ConfigNodeMessages.FAILED_TO_DROP_UDF_THIS_UDF_HAS_NOT_BEEN_CREATED, udfName));
  }

  public boolean needToSaveJar(String jarName) {
    return !existedJarToMD5.containsKey(jarName);
  }

  public TSStatus addUDFInTable(CreateFunctionPlan physicalPlan) {
    try {
      final UDFInformation udfInformation = physicalPlan.getUdfInformation();
      udfTable.addUDFInformation(udfInformation.getFunctionName(), udfInformation);
      if (udfInformation.isUsingURI()) {
        existedJarToMD5.put(udfInformation.getJarName(), udfInformation.getJarMD5());
        if (physicalPlan.getJarFile() != null) {
          udfExecutableManager.saveToInstallDir(
              ByteBuffer.wrap(physicalPlan.getJarFile().getValues()), udfInformation.getJarName());
        }
      }
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (Exception e) {
      final String errorMessage =
          String.format(
              "Failed to add UDF [%s] in UDF_Table on Config Nodes, because of %s",
              physicalPlan.getUdfInformation().getFunctionName(), e);
      LOGGER.warn(errorMessage, e);
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage(errorMessage);
    }
  }

  public DataSet getUDFTable(GetFunctionTablePlan plan) {
    return new FunctionTableResp(
        new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()),
        udfTable.getUDFInformationList(plan.getModel()));
  }

  public DataSet getAllUDFTable() {
    return new FunctionTableResp(
        new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()),
        udfTable.getAllInformationList());
  }

  public JarResp getUDFJar(GetUDFJarPlan physicalPlan) {
    List<ByteBuffer> jarList = new ArrayList<>();
    try {
      for (String jarName : physicalPlan.getJarNames()) {
        jarList.add(
            ExecutableManager.transferToBytebuffer(
                UDFExecutableManager.getInstance().getFileStringUnderInstallByName(jarName)));
      }
    } catch (Exception e) {
      LOGGER.error(ConfigNodeMessages.GET_UDF_JAR_FAILED, e);
      return new JarResp(
          new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
              .setMessage("Get UDF_Jar failed, because " + e.getMessage()),
          Collections.emptyList());
    }
    return new JarResp(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()), jarList);
  }

  public TSStatus dropFunction(Model model, String functionName) {
    if (udfTable.containsUDF(model, functionName)) {
      existedJarToMD5.remove(udfTable.getUDFInformation(model, functionName).getJarName());
      udfTable.removeUDFInformation(model, functionName);
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public TSStatus updateFunction(UpdateFunctionPlan req) {
    UDFInformation udfInformation = req.getUdfInformation();
    udfTable.addUDFInformation(udfInformation.getFunctionName(), udfInformation);
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  @TestOnly
  public Map<Model, Map<String, UDFInformation>> getRawUDFTable() {
    return udfTable.getTable();
  }

  @TestOnly
  public Map<String, String> getRawExistedJarToMD5() {
    return existedJarToMD5;
  }

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws IOException {
    File snapshotFile = new File(snapshotDir, SNAPSHOT_FILENAME);
    if (snapshotFile.exists() && snapshotFile.isFile()) {
      LOGGER.error(
          ConfigNodeMessages.FAILED_TO_TAKE_SNAPSHOT_BECAUSE_SNAPSHOT_FILE_IS_ALREADY_EXIST,
          snapshotFile.getAbsolutePath());
      return false;
    }

    try (FileOutputStream fileOutputStream = new FileOutputStream(snapshotFile)) {

      serializeExistedJarToMD5(fileOutputStream);

      udfTable.serializeUDFTable(fileOutputStream);

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
          ConfigNodeMessages.FAILED_TO_LOAD_SNAPSHOT_SNAPSHOT_FILE_IS_NOT_EXIST_2,
          snapshotFile.getAbsolutePath());
      return;
    }

    acquireUDFTableLock();
    try (FileInputStream fileInputStream = new FileInputStream(snapshotFile)) {

      clear();

      deserializeExistedJarToMD5(fileInputStream);

      udfTable.deserializeUDFTable(fileInputStream);
    } finally {
      releaseUDFTableLock();
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

  public void clear() {
    existedJarToMD5.clear();
    udfTable.clear();
  }
}
