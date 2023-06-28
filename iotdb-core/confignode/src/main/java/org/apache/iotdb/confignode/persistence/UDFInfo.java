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
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.commons.udf.UDFInformation;
import org.apache.iotdb.commons.udf.UDFTable;
import org.apache.iotdb.commons.udf.service.UDFExecutableManager;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.read.udf.GetUDFJarPlan;
import org.apache.iotdb.confignode.consensus.request.write.function.CreateFunctionPlan;
import org.apache.iotdb.confignode.consensus.request.write.function.DropFunctionPlan;
import org.apache.iotdb.confignode.consensus.response.function.FunctionTableResp;
import org.apache.iotdb.confignode.consensus.response.udf.JarResp;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.udf.api.exception.UDFManagementException;

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

  private static final String snapshotFileName = "udf_info.bin";

  public UDFInfo() throws IOException {
    udfTable = new UDFTable();
    existedJarToMD5 = new HashMap<>();
    udfExecutableManager =
        UDFExecutableManager.setupAndGetInstance(
            CONFIG_NODE_CONF.getUdfTemporaryLibDir(), CONFIG_NODE_CONF.getUdfDir());
  }

  public void acquireUDFTableLock() {
    LOGGER.info("acquire UDFTableLock");
    udfTableLock.lock();
  }

  public void releaseUDFTableLock() {
    LOGGER.info("release UDFTableLock");
    udfTableLock.unlock();
  }

  /** Validate whether the UDF can be created */
  public void validate(String UDFName, String jarName, String jarMD5) {
    if (udfTable.containsUDF(UDFName)) {
      throw new UDFManagementException(
          String.format("Failed to create UDF [%s], the same name UDF has been created", UDFName));
    }

    if (existedJarToMD5.containsKey(jarName) && !existedJarToMD5.get(jarName).equals(jarMD5)) {
      throw new UDFManagementException(
          String.format(
              "Failed to create UDF [%s], the same name Jar [%s] but different MD5 [%s] has existed",
              UDFName, jarName, jarMD5));
    }
  }

  /** Validate whether the UDF can be dropped */
  public void validate(String udfName) {
    if (udfTable.containsUDF(udfName)) {
      return;
    }
    throw new UDFManagementException(
        String.format("Failed to drop UDF [%s], this UDF has not been created", udfName));
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

  public DataSet getUDFTable() {
    return new FunctionTableResp(
        new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()),
        udfTable.getAllNonBuiltInUDFInformation());
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
      LOGGER.error("Get UDF_Jar failed", e);
      return new JarResp(
          new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
              .setMessage("Get UDF_Jar failed, because " + e.getMessage()),
          Collections.emptyList());
    }
    return new JarResp(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()), jarList);
  }

  public TSStatus dropFunction(DropFunctionPlan req) {
    String udfName = req.getFunctionName();
    if (udfTable.containsUDF(udfName)) {
      existedJarToMD5.remove(udfTable.getUDFInformation(udfName).getJarName());
      udfTable.removeUDFInformation(udfName);
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  @TestOnly
  public Map<String, UDFInformation> getRawUDFTable() {
    return udfTable.getTable();
  }

  @TestOnly
  public Map<String, String> getRawExistedJarToMD5() {
    return existedJarToMD5;
  }

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws IOException {
    File snapshotFile = new File(snapshotDir, snapshotFileName);
    if (snapshotFile.exists() && snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to take snapshot, because snapshot file [{}] is already exist.",
          snapshotFile.getAbsolutePath());
      return false;
    }

    acquireUDFTableLock();
    try (FileOutputStream fileOutputStream = new FileOutputStream(snapshotFile)) {

      serializeExistedJarToMD5(fileOutputStream);

      udfTable.serializeUDFTable(fileOutputStream);

      return true;
    } finally {
      releaseUDFTableLock();
    }
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws IOException {
    File snapshotFile = new File(snapshotDir, snapshotFileName);
    if (!snapshotFile.exists() || !snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to load snapshot,snapshot file [{}] is not exist.",
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
