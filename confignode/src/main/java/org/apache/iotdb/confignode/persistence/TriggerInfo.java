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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.commons.trigger.TriggerInformation;
import org.apache.iotdb.commons.trigger.TriggerTable;
import org.apache.iotdb.commons.trigger.exception.TriggerManagementException;
import org.apache.iotdb.commons.trigger.service.TriggerExecutableManager;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.consensus.request.write.trigger.AddTriggerInTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.trigger.DeleteTriggerInTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.trigger.UpdateTriggerStateInTablePlan;
import org.apache.iotdb.confignode.consensus.response.TriggerTableResp;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
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

  private final String snapshotFileName = "trigger_info.bin";

  public TriggerInfo() throws IOException {
    triggerTable = new TriggerTable();
    existedJarToMD5 = new HashMap<>();
    // jarReferenceTable = new ConcurrentHashMap<>();
    triggerExecutableManager =
        TriggerExecutableManager.setupAndGetInstance(
            CONFIG_NODE_CONF.getTemporaryLibDir(), CONFIG_NODE_CONF.getTriggerLibDir());
  }

  public void acquireTriggerTableLock() {
    LOGGER.info("acquire TriggerTableLock");
    triggerTableLock.lock();
  }

  public void releaseTriggerTableLock() {
    LOGGER.info("release TriggerTableLock");
    triggerTableLock.unlock();
  }

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws TException, IOException {
    // TODO implement when 'Drop Trigger' done
    return true;
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws TException, IOException {
    // TODO implement when 'Drop Trigger' done
  }

  /**
   * Validate whether the trigger can be created
   *
   * @param triggerName
   * @param jarName
   * @param jarMD5
   */
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

  /**
   * Validate whether the trigger can be dropped
   *
   * @param triggerName
   */
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
      existedJarToMD5.put(triggerInformation.getJarName(), triggerInformation.getJarFileMD5());
      if (physicalPlan.getJarFile() != null) {
        triggerExecutableManager.writeToLibDir(
            ByteBuffer.wrap(physicalPlan.getJarFile().getValues()),
            triggerInformation.getJarName());
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

  public TriggerTableResp getTriggerTable() {
    return new TriggerTableResp(
        new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()),
        triggerTable.getAllTriggerInformation());
  }
}
