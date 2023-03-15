/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.confignode.persistence;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.executable.ExecutableManager;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.commons.trigger.TriggerInformation;
import org.apache.iotdb.commons.trigger.TriggerTable;
import org.apache.iotdb.commons.trigger.exception.TriggerManagementException;
import org.apache.iotdb.commons.trigger.service.TriggerExecutableManager;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.read.trigger.GetTriggerJarPlan;
import org.apache.iotdb.confignode.consensus.request.read.trigger.GetTriggerLocationPlan;
import org.apache.iotdb.confignode.consensus.request.read.trigger.GetTriggerTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.trigger.AddTriggerInTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.trigger.DeleteTriggerInTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.trigger.UpdateTriggerLocationPlan;
import org.apache.iotdb.confignode.consensus.request.write.trigger.UpdateTriggerStateInTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.trigger.UpdateTriggersOnTransferNodesPlan;
import org.apache.iotdb.confignode.consensus.response.trigger.TransferringTriggersResp;
import org.apache.iotdb.confignode.consensus.response.trigger.TriggerLocationResp;
import org.apache.iotdb.confignode.consensus.response.trigger.TriggerTableResp;
import org.apache.iotdb.confignode.consensus.response.udf.JarResp;
import org.apache.iotdb.confignode.rpc.thrift.TTriggerState;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.thrift.TException;
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
import java.util.UUID;
import java.util.concurrent.locks.ReentrantLock;

public class TriggerInfo implements SnapshotProcessor {
  private static final Logger LOGGER = LoggerFactory.getLogger(TriggerInfo.class);

  private static final ConfigNodeConfig CONFIG_NODE_CONF =
      ConfigNodeDescriptor.getInstance().getConf();

  private final TriggerTable triggerTable;
  private final Map<String, String> existedJarToMD5;
  // private final Map<String, AtomicInteger> jarReferenceTable;

  private final TriggerExecutableManager triggerExecutableManager;

  private final ReentrantLock triggerTableLock = new ReentrantLock();

  private static final String SNAPSHOT_FILENAME = "trigger_info.bin";

  public TriggerInfo() throws IOException {
    triggerTable = new TriggerTable();
    existedJarToMD5 = new HashMap<>();
    // jarReferenceTable = new ConcurrentHashMap<>();
    triggerExecutableManager =
        TriggerExecutableManager.setupAndGetInstance(
            CONFIG_NODE_CONF.getTriggerTemporaryLibDir(), CONFIG_NODE_CONF.getTriggerDir());
  }

  public void acquireTriggerTableLock() {
    LOGGER.info("acquire TriggerTableLock");
    triggerTableLock.lock();
  }

  public void releaseTriggerTableLock() {
    LOGGER.info("release TriggerTableLock");
    triggerTableLock.unlock();
  }

  /** Validate whether the trigger can be created */
  public void validate(String triggerName, String jarName, String jarMD5) {
    if (triggerTable.containsTrigger(triggerName)) {
      throw new TriggerManagementException(
          String.format(
              "Failed to create trigger [%s], the same name trigger has been created",
              triggerName));
    }

    if (existedJarToMD5.containsKey(jarName) && !existedJarToMD5.get(jarName).equals(jarMD5)) {
      throw new TriggerManagementException(
          String.format(
              "Failed to create trigger [%s], the same name Jar [%s] but different MD5 [%s] has existed",
              triggerName, jarName, jarMD5));
    }
  }

  /** Validate whether the trigger can be dropped */
  public void validate(String triggerName) {
    if (triggerTable.containsTrigger(triggerName)) {
      return;
    }
    throw new TriggerManagementException(
        String.format(
            "Failed to drop trigger [%s], this trigger has not been created", triggerName));
  }

  public boolean needToSaveJar(String jarName) {
    return !existedJarToMD5.containsKey(jarName);
  }

  public TSStatus addTriggerInTable(AddTriggerInTablePlan physicalPlan) {
    try {
      TriggerInformation triggerInformation = physicalPlan.getTriggerInformation();
      triggerTable.addTriggerInformation(triggerInformation.getTriggerName(), triggerInformation);
      if (triggerInformation.isUsingURI()) {
        existedJarToMD5.put(triggerInformation.getJarName(), triggerInformation.getJarFileMD5());
        if (physicalPlan.getJarFile() != null) {
          triggerExecutableManager.saveToInstallDir(
              ByteBuffer.wrap(physicalPlan.getJarFile().getValues()),
              triggerInformation.getJarName());
        }
      }
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } catch (Exception e) {
      final String errorMessage =
          String.format(
              "Failed to add trigger [%s] in TriggerTable on Config Nodes, because of %s",
              physicalPlan.getTriggerInformation().getTriggerName(), e);
      LOGGER.warn(errorMessage, e);
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage(errorMessage);
    }
  }

  public TSStatus deleteTriggerInTable(DeleteTriggerInTablePlan physicalPlan) {
    String triggerName = physicalPlan.getTriggerName();
    if (triggerTable.containsTrigger(triggerName)) {
      existedJarToMD5.remove(triggerTable.getTriggerInformation(triggerName).getJarName());
      triggerTable.deleteTriggerInformation(triggerName);
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public TSStatus updateTriggerStateInTable(UpdateTriggerStateInTablePlan physicalPlan) {
    triggerTable.setTriggerState(physicalPlan.getTriggerName(), physicalPlan.getTriggerState());
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public TriggerTableResp getTriggerTable(GetTriggerTablePlan req) {
    if (req.isOnlyStateful()) {
      return new TriggerTableResp(
          new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()),
          triggerTable.getAllStatefulTriggerInformation());
    } else {
      return new TriggerTableResp(
          new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()),
          triggerTable.getAllTriggerInformation());
    }
  }

  public DataSet getTriggerLocation(GetTriggerLocationPlan req) {
    TDataNodeLocation dataNodeLocation = triggerTable.getTriggerLocation(req.getTriggerName());
    if (dataNodeLocation != null) {
      return new TriggerLocationResp(
          new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()), dataNodeLocation);
    } else {
      return new TriggerLocationResp(
          new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
              .setMessage(String.format("Fail to get Location trigger[%s]", req.getTriggerName())),
          null);
    }
  }

  public JarResp getTriggerJar(GetTriggerJarPlan physicalPlan) {
    List<ByteBuffer> jarList = new ArrayList<>();
    try {
      for (String jarName : physicalPlan.getJarNames()) {
        jarList.add(
            ExecutableManager.transferToBytebuffer(
                TriggerExecutableManager.getInstance().getFileStringUnderInstallByName(jarName)));
      }
    } catch (Exception e) {
      LOGGER.error("Get TriggerJar failed", e);
      return new JarResp(
          new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
              .setMessage("Get TriggerJar failed, because " + e.getMessage()),
          Collections.emptyList());
    }
    return new JarResp(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()), jarList);
  }

  public TransferringTriggersResp getTransferringTriggers() {
    return new TransferringTriggersResp(triggerTable.getTransferringTriggers());
  }

  public TSStatus updateTriggersOnTransferNodes(UpdateTriggersOnTransferNodesPlan physicalPlan) {
    triggerTable.updateTriggersOnTransferNodes(physicalPlan.getDataNodeLocations());
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public TSStatus updateTriggerLocation(UpdateTriggerLocationPlan physicalPlan) {
    triggerTable.updateTriggerLocation(
        physicalPlan.getTriggerName(), physicalPlan.getDataNodeLocation());
    triggerTable.setTriggerState(physicalPlan.getTriggerName(), TTriggerState.ACTIVE);
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  @TestOnly
  public Map<String, TriggerInformation> getRawTriggerTable() {
    return triggerTable.getTable();
  }

  @TestOnly
  public Map<String, String> getRawExistedJarToMD5() {
    return existedJarToMD5;
  }

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws TException, IOException {
    File snapshotFile = new File(snapshotDir, SNAPSHOT_FILENAME);
    if (snapshotFile.exists() && snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to take snapshot, because snapshot file [{}] is already exist.",
          snapshotFile.getAbsolutePath());
      return false;
    }
    File tmpFile = new File(snapshotFile.getAbsolutePath() + "-" + UUID.randomUUID());

    acquireTriggerTableLock();
    try (FileOutputStream fileOutputStream = new FileOutputStream(tmpFile)) {

      serializeExistedJarToMD5(fileOutputStream);

      triggerTable.serializeTriggerTable(fileOutputStream);

      fileOutputStream.flush();
      fileOutputStream.close();

      return tmpFile.renameTo(snapshotFile);
    } finally {
      releaseTriggerTableLock();
      for (int retry = 0; retry < 5; retry++) {
        if (!tmpFile.exists() || tmpFile.delete()) {
          break;
        } else {
          LOGGER.warn(
              "Can't delete temporary snapshot file: {}, retrying...", tmpFile.getAbsolutePath());
        }
      }
    }
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws TException, IOException {
    File snapshotFile = new File(snapshotDir, SNAPSHOT_FILENAME);
    if (!snapshotFile.exists() || !snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to load snapshot,snapshot file [{}] is not exist.",
          snapshotFile.getAbsolutePath());
      return;
    }

    acquireTriggerTableLock();
    try (FileInputStream fileInputStream = new FileInputStream(snapshotFile)) {

      clear();

      deserializeExistedJarToMD5(fileInputStream);

      triggerTable.deserializeTriggerTable(fileInputStream);
    } finally {
      releaseTriggerTableLock();
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
    triggerTable.clear();
  }
}
