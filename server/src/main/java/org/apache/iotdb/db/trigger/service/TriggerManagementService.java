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

package org.apache.iotdb.db.trigger.service;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PatternTreeMap;
import org.apache.iotdb.commons.trigger.TriggerInformation;
import org.apache.iotdb.commons.trigger.TriggerTable;
import org.apache.iotdb.commons.trigger.exception.TriggerExecutionException;
import org.apache.iotdb.commons.trigger.exception.TriggerManagementException;
import org.apache.iotdb.commons.trigger.service.TriggerExecutableManager;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.confignode.rpc.thrift.TTriggerState;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.path.PatternTreeMapFactory;
import org.apache.iotdb.db.trigger.executor.TriggerExecutor;
import org.apache.iotdb.trigger.api.Trigger;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class TriggerManagementService {

  private static final Logger LOGGER = LoggerFactory.getLogger(TriggerManagementService.class);

  private final ReentrantLock lock;

  private final TriggerTable triggerTable;

  private final Map<String, TriggerExecutor> executorMap;

  /**
   * Maintain a PatternTree: PathPattern -> List<String> triggerNames Return the triggerNames of
   * triggers whose PathPatterns match the given one.
   */
  private final PatternTreeMap<String, PatternTreeMapFactory.StringSerializer> patternTreeMap;

  private static final int DATA_NODE_ID = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();

  private TriggerManagementService() {
    this.lock = new ReentrantLock();
    this.triggerTable = new TriggerTable();
    this.executorMap = new ConcurrentHashMap<>();
    this.patternTreeMap = PatternTreeMapFactory.getTriggerPatternTreeMap();
  }

  public void acquireLock() {
    lock.lock();
  }

  public void releaseLock() {
    lock.unlock();
  }

  public void register(TriggerInformation triggerInformation, ByteBuffer jarFile)
      throws IOException {
    try {
      acquireLock();
      checkIfRegistered(triggerInformation);
      saveJarFile(triggerInformation.getJarName(), jarFile);
      doRegister(triggerInformation, false);
    } finally {
      releaseLock();
    }
  }

  public void activeTrigger(String triggerName) {
    try {
      acquireLock();
      triggerTable.setTriggerState(triggerName, TTriggerState.ACTIVE);
    } finally {
      releaseLock();
    }
  }

  public void inactiveTrigger(String triggerName) {
    try {
      acquireLock();
      triggerTable.setTriggerState(triggerName, TTriggerState.INACTIVE);
    } finally {
      releaseLock();
    }
  }

  public void dropTrigger(String triggerName, boolean needToDeleteJar) throws IOException {
    try {
      acquireLock();
      TriggerInformation triggerInformation = triggerTable.removeTriggerInformation(triggerName);
      TriggerExecutor executor = executorMap.remove(triggerName);
      if (executor != null) {
        executor.onDrop();
      }
      if (triggerInformation == null) {
        return;
      }
      patternTreeMap.delete(triggerInformation.getPathPattern(), triggerName);

      // if it is needed to delete jar file of the trigger, delete both jar file and md5
      if (needToDeleteJar) {
        TriggerExecutableManager.getInstance()
            .removeFileUnderLibRoot(triggerInformation.getJarName());
        TriggerExecutableManager.getInstance().removeFileUnderTemporaryRoot(triggerName + ".txt");
      }
    } catch (TriggerExecutionException ignored) {
      // Drop trigger can success even onDrop throw an exception for now
    } finally {
      releaseLock();
    }
  }

  public void updateLocationOfStatefulTrigger(String triggerName, TDataNodeLocation newLocation)
      throws IOException {
    try {
      acquireLock();
      TriggerInformation triggerInformation = triggerTable.getTriggerInformation(triggerName);
      if (triggerInformation == null || !triggerInformation.isStateful()) {
        return;
      }
      triggerInformation.setDataNodeLocation(newLocation);
      triggerTable.addTriggerInformation(triggerName, triggerInformation);
      if (newLocation.getDataNodeId() != DATA_NODE_ID) {
        // The instance of stateful trigger is created on another DataNode. We need to drop the
        // instance if it exists on this DataNode
        TriggerExecutor triggerExecutor = executorMap.remove(triggerName);
        if (triggerExecutor != null) {
          triggerExecutor.onDrop();
        }
      } else {
        TriggerExecutor triggerExecutor = executorMap.get(triggerName);
        if (triggerExecutor != null) {
          return;
        }
        // newLocation of stateful trigger is this DataNode, we need to create its instance if it
        // does not exist.
        try (TriggerClassLoader currentActiveClassLoader =
            TriggerClassLoaderManager.getInstance().updateAndGetActiveClassLoader()) {
          TriggerExecutor newExecutor =
              new TriggerExecutor(
                  triggerInformation,
                  constructTriggerInstance(
                      triggerInformation.getClassName(), currentActiveClassLoader),
                  true);
          executorMap.put(triggerName, newExecutor);
        }
      }
    } finally {
      releaseLock();
    }
  }

  public boolean isTriggerTableEmpty() {
    return triggerTable.isEmpty();
  }

  public TriggerTable getTriggerTable() {
    return triggerTable;
  }

  public TriggerExecutor getExecutor(String triggerName) {
    return executorMap.get(triggerName);
  }

  public boolean needToFireOnAnotherDataNode(String triggerName) {
    TriggerInformation triggerInformation = triggerTable.getTriggerInformation(triggerName);
    return triggerInformation.isStateful()
        && triggerInformation.getDataNodeLocation().getDataNodeId() != DATA_NODE_ID;
  }

  public TriggerInformation getTriggerInformation(String triggerName) {
    return triggerTable.getTriggerInformation(triggerName);
  }

  /**
   * @param devicePath PathPattern
   * @return all the triggers that matched this Pattern
   */
  public List<List<String>> getMatchedTriggerListForPath(
      PartialPath devicePath, List<String> measurements) {
    return patternTreeMap.getOverlapped(devicePath, measurements);
  }

  private void checkIfRegistered(TriggerInformation triggerInformation)
      throws TriggerManagementException {
    String triggerName = triggerInformation.getTriggerName();
    String jarName = triggerInformation.getJarName();
    if (triggerTable.containsTrigger(triggerName)
        && TriggerExecutableManager.getInstance().hasFileUnderLibRoot(jarName)) {
      if (isLocalJarConflicted(triggerInformation)) {
        // same jar name with different md5
        String errorMessage =
            String.format(
                "Failed to registered trigger %s, "
                    + "because existed md5 of jar file for trigger %s is different from the new jar file. ",
                triggerName, triggerName);
        LOGGER.warn(errorMessage);
        throw new TriggerManagementException(errorMessage);
      }
    }
  }

  /** check whether local jar is correct according to md5 */
  public boolean isLocalJarConflicted(TriggerInformation triggerInformation)
      throws TriggerManagementException {
    String triggerName = triggerInformation.getTriggerName();
    // A jar with the same name exists, we need to check md5
    String existedMd5 = "";
    String md5FilePath = triggerName + ".txt";

    // if meet error when reading md5 from txt, we need to compute it again
    boolean hasComputed = false;
    if (TriggerExecutableManager.getInstance().hasFileUnderTemporaryRoot(md5FilePath)) {
      try {
        existedMd5 =
            TriggerExecutableManager.getInstance().readTextFromFileUnderTemporaryRoot(md5FilePath);
        hasComputed = true;
      } catch (IOException e) {
        LOGGER.warn("Error occurred when trying to read md5 of {}", md5FilePath);
      }
    }
    if (!hasComputed) {
      try {
        existedMd5 =
            DigestUtils.md5Hex(
                Files.newInputStream(
                    Paths.get(
                        TriggerExecutableManager.getInstance().getInstallDir()
                            + File.separator
                            + triggerInformation.getJarName())));
        // save the md5 in a txt under trigger temporary lib
        TriggerExecutableManager.getInstance()
            .saveTextAsFileUnderTemporaryRoot(existedMd5, md5FilePath);
      } catch (IOException e) {
        String errorMessage =
            String.format(
                "Failed to registered trigger %s, "
                    + "because error occurred when trying to compute md5 of jar file for trigger %s ",
                triggerName, triggerName);
        LOGGER.warn(errorMessage, e);
        throw new TriggerManagementException(errorMessage);
      }
    }
    return !existedMd5.equals(triggerInformation.getJarFileMD5());
  }

  private void saveJarFile(String jarName, ByteBuffer byteBuffer) throws IOException {
    if (byteBuffer != null) {
      TriggerExecutableManager.getInstance().saveToInstallDir(byteBuffer, jarName);
    }
  }

  /**
   * Only call this method directly for registering new data node, otherwise you need to call
   * register().
   */
  public void doRegister(TriggerInformation triggerInformation, boolean isRestoring)
      throws IOException {
    try (TriggerClassLoader currentActiveClassLoader =
        TriggerClassLoaderManager.getInstance().updateAndGetActiveClassLoader()) {
      String triggerName = triggerInformation.getTriggerName();
      // register in trigger-table
      triggerTable.addTriggerInformation(triggerName, triggerInformation);
      // update PatternTreeMap
      patternTreeMap.append(triggerInformation.getPathPattern(), triggerName);
      // if it is a stateful trigger, we only maintain its instance on specified DataNode
      if (!triggerInformation.isStateful()
          || triggerInformation.getDataNodeLocation().getDataNodeId() == DATA_NODE_ID) {
        // get trigger instance
        Trigger trigger =
            constructTriggerInstance(triggerInformation.getClassName(), currentActiveClassLoader);
        // construct and save TriggerExecutor after successfully creating trigger instance
        TriggerExecutor triggerExecutor =
            new TriggerExecutor(triggerInformation, trigger, isRestoring);
        executorMap.put(triggerName, triggerExecutor);
      }
    } catch (Exception e) {
      String errorMessage =
          String.format(
              "Failed to register trigger %s with className: %s. The cause is: %s",
              triggerInformation.getTriggerName(), triggerInformation.getClassName(), e);
      LOGGER.warn(errorMessage);
      throw e;
    }
  }

  public Trigger constructTriggerInstance(String className, TriggerClassLoader classLoader)
      throws TriggerManagementException {
    try {
      Class<?> triggerClass = Class.forName(className, true, classLoader);
      return (Trigger) triggerClass.getDeclaredConstructor().newInstance();
    } catch (InstantiationException
        | InvocationTargetException
        | NoSuchMethodException
        | IllegalAccessException
        | ClassNotFoundException
        | ClassCastException e) {
      throw new TriggerManagementException(
          String.format(
              "Failed to reflect trigger instance with className(%s), because %s", className, e));
    }
  }

  /**
   * @param triggerName given trigger
   * @return TDataNodeLocation of DataNode where instance of given stateful trigger is on. Null if
   *     trigger not found.
   */
  public TDataNodeLocation getDataNodeLocationOfStatefulTrigger(String triggerName) {
    TriggerInformation triggerInformation = triggerTable.getTriggerInformation(triggerName);
    if (triggerInformation.isStateful()) {
      return triggerInformation.getDataNodeLocation();
    }
    return null;
  }

  // region only for test

  @TestOnly
  public List<TriggerInformation> getAllTriggerInformationInTriggerTable() {
    return triggerTable.getAllTriggerInformation();
  }

  @TestOnly
  public List<TriggerExecutor> getAllTriggerExecutors() {
    return new ArrayList<>(executorMap.values());
  }

  // end region

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // singleton instance holder
  /////////////////////////////////////////////////////////////////////////////////////////////////
  private static class TriggerManagementServiceHolder {
    private static final TriggerManagementService INSTANCE = new TriggerManagementService();
  }

  public static TriggerManagementService getInstance() {
    return TriggerManagementServiceHolder.INSTANCE;
  }
}
