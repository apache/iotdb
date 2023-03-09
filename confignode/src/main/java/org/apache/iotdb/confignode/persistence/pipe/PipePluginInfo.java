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

package org.apache.iotdb.confignode.persistence.pipe;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.executable.ExecutableManager;
import org.apache.iotdb.commons.pipe.plugin.PipePluginInformation;
import org.apache.iotdb.commons.pipe.plugin.PipePluginTable;
import org.apache.iotdb.commons.pipe.plugin.service.PipePluginExecutableManager;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.read.pipe.plugin.GetPipePluginJarPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.plugin.CreatePipePluginPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.plugin.DropPipePluginPlan;
import org.apache.iotdb.confignode.consensus.response.pipe.plugin.PipePluginTableResp;
import org.apache.iotdb.confignode.consensus.response.udf.JarResp;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.pipe.api.exception.PipeManagementException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class PipePluginInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipePluginInfo.class);

  private static final ConfigNodeConfig CONFIG_NODE_CONF =
      ConfigNodeDescriptor.getInstance().getConf();

  private final PipePluginTable pipePluginTable;

  private final Map<String, String> existedJarToMD5;

  private final PipePluginExecutableManager pipePluginExecutableManager;

  private final ReentrantLock pipePluginTableLock = new ReentrantLock();

  private static final String snapshotFileName = "pipe_plugin_info.bin";

  public PipePluginInfo() throws IOException {
    pipePluginTable = new PipePluginTable();
    existedJarToMD5 = new HashMap<>();
    pipePluginExecutableManager =
        PipePluginExecutableManager.setupAndGetInstance(
            CONFIG_NODE_CONF.getPipeTemporaryLibDir(), CONFIG_NODE_CONF.getPipeDir());
  }

  public void acquirePipePluginTableLock() {
    pipePluginTableLock.lock();
  }

  public void releasePipePluginTableLock() {
    pipePluginTableLock.unlock();
  }

  public void validate(String pluginName, String jarName, String jarMD5) {
    if (pipePluginTable.containsPipePlugin(pluginName)) {
      throw new PipeManagementException(
          String.format(
              "Failed to create PipePlugin [%s], the same name PipePlugin has been created",
              pluginName));
    }

    if (existedJarToMD5.containsKey(jarName) && !existedJarToMD5.get(jarName).equals(jarMD5)) {
      throw new PipeManagementException(
          String.format(
              "Failed to create PipePlugin [%s], the same name Jar [%s] but different MD5 [%s] has existed",
              pluginName, jarName, jarMD5));
    }
  }

  /** Validate whether the PipePlugin can be dropped */
  public void validate(String pluginName) {
    if (pipePluginTable.containsPipePlugin(pluginName)) {
      return;
    }
    throw new PipeManagementException(
        String.format("Failed to drop PipePlugin [%s], the PipePlugin does not exist", pluginName));
  }

  public boolean needToSaveJar(String jarName) {
    return !existedJarToMD5.containsKey(jarName);
  }

  public TSStatus addPipePluginInTable(CreatePipePluginPlan physicalPlan) {
    try {
      final PipePluginInformation pipePluginInformation = physicalPlan.getPipePluginInformation();
      pipePluginTable.addPipePluginInformation(
          pipePluginInformation.getPluginName(), pipePluginInformation);
      existedJarToMD5.put(pipePluginInformation.getJarName(), pipePluginInformation.getJarMD5());
      if (physicalPlan.getJarFile() != null) {
        pipePluginExecutableManager.saveToInstallDir(
            ByteBuffer.wrap(physicalPlan.getJarFile().getValues()),
            pipePluginInformation.getJarName());
      }
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (Exception e) {
      final String errorMessage =
          String.format(
              "Failed to add PipePlugin [%s] in PipePlugin_Table on Config Nodes, because of %s",
              physicalPlan.getPipePluginInformation().getPluginName(), e);
      LOGGER.warn(errorMessage, e);
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage(errorMessage);
    }
  }

  public DataSet getPipePluginTable() {
    return new PipePluginTableResp(
        new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()),
        Arrays.asList(pipePluginTable.getAllPipePluginInformation()));
  }

  public JarResp getPipePluginJar(GetPipePluginJarPlan physicalPlan) {
    List<ByteBuffer> jarList = new ArrayList<>();

    try {
      for (String jarName : physicalPlan.getJarNames()) {
        jarList.add(
            ExecutableManager.transferToBytebuffer(
                PipePluginExecutableManager.getInstance()
                    .getFileStringUnderInstallByName(jarName)));
      }
    } catch (Exception e) {
      LOGGER.error("Get PipePlugin_Jar failed", e);
      return new JarResp(
          new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
              .setMessage("Get PipePlugin_Jar failed, because " + e.getMessage()),
          Collections.emptyList());
    }
    return new JarResp(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()), jarList);
  }

  public TSStatus dropPipePlugin(DropPipePluginPlan physicalPlan) {
    String pluginName = physicalPlan.getPluginName();
    if (pipePluginTable.containsPipePlugin(pluginName)) {
      existedJarToMD5.remove(pipePluginTable.getPipePluginInformation(pluginName).getJarName());
      pipePluginTable.removePipePluginInformation(pluginName);
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
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

    acquirePipePluginTableLock();
    try (FileOutputStream fileOutputStream = new FileOutputStream(snapshotFile)) {

      serializeExistedJarToMD5(fileOutputStream);

      pipePluginTable.serializePipePluginTable(fileOutputStream);

      return true;
    } finally {
      releasePipePluginTableLock();
    }
  }

  public void serializeExistedJarToMD5(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(existedJarToMD5.size(), outputStream);
    for (Map.Entry<String, String> entry : existedJarToMD5.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      ReadWriteIOUtils.write(entry.getValue(), outputStream);
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

    acquirePipePluginTableLock();
    try (FileInputStream fileInputStream = new FileInputStream(snapshotFile)) {

      clear();

      deserializeExistedJarToMD5(fileInputStream);

      pipePluginTable.deserializePipePluginTable(fileInputStream);
    } finally {
      releasePipePluginTableLock();
    }
  }

  public void deserializeExistedJarToMD5(InputStream inputStream) throws IOException {
    int size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; i++) {
      existedJarToMD5.put(
          ReadWriteIOUtils.readString(inputStream), ReadWriteIOUtils.readString(inputStream));
    }
  }

  public void clear() {
    existedJarToMD5.clear();
    pipePluginTable.clear();
  }
}
