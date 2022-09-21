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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternNode;
import org.apache.iotdb.commons.path.PatternTreeMap;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.trigger.TriggerInformation;
import org.apache.iotdb.commons.trigger.TriggerTable;
import org.apache.iotdb.commons.trigger.exception.TriggerManagementException;
import org.apache.iotdb.commons.trigger.service.TriggerClassLoader;
import org.apache.iotdb.commons.trigger.service.TriggerClassLoaderManager;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.path.PatternTreeMapFactory;
import org.apache.iotdb.db.trigger.executor.TriggerExecutor;
import org.apache.iotdb.trigger.api.Trigger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class TriggerManagementService implements IService {

  private static final Logger LOGGER = LoggerFactory.getLogger(TriggerManagementService.class);

  private final ReentrantLock registrationLock;

  private final TriggerTable triggerTable;

  private final Map<String, TriggerExecutor> executorMap;

  /**
   * Maintain a PatternTree: PathPattern -> List<String> triggerNames Return the triggerNames of
   * triggers whose PathPatterns match the given one.
   */
  private final PatternTreeMap<String, PathPatternNode.StringSerializer> patternTreeMap;

  private static final int DATA_NODE_ID = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();

  private TriggerManagementService() {
    this.registrationLock = new ReentrantLock();
    this.triggerTable = new TriggerTable();
    this.executorMap = new ConcurrentHashMap<>();
    this.patternTreeMap = PatternTreeMapFactory.getTriggerPatternTreeMap();
  }

  public void acquireRegistrationLock() {
    registrationLock.lock();
  }

  public void releaseRegistrationLock() {
    registrationLock.unlock();
  }

  public void register(TriggerInformation triggerInformation) {
    acquireRegistrationLock();
    checkIfRegistered(triggerInformation);
    doRegister(triggerInformation);
    releaseRegistrationLock();
  };

  public void activeTrigger(String triggerName) {
    triggerTable.activeTrigger(triggerName);
  }

  public boolean isTriggerTableEmpty() {
    return triggerTable.isEmpty();
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

  public List<String> getMatchedTriggerListForPath(PartialPath fullPath) {
    return patternTreeMap.getOverlapped(fullPath);
  }

  private void checkIfRegistered(TriggerInformation triggerInformation)
      throws TriggerManagementException {
    String triggerName = triggerInformation.getTriggerName();
    if (triggerTable.containsTrigger(triggerName)) {
      String errorMessage =
          String.format(
              "Failed to registered trigger %s, "
                  + "because trigger %s has already been registered in TriggerTable",
              triggerName, triggerName);
      LOGGER.warn(errorMessage);
      throw new TriggerManagementException(errorMessage);
    }
  }

  private void doRegister(TriggerInformation triggerInformation) {
    try (TriggerClassLoader currentActiveClassLoader =
        TriggerClassLoaderManager.getInstance().updateAndGetActiveClassLoader()) {
      String triggerName = triggerInformation.getTriggerName();
      // get trigger instance
      Trigger trigger =
          constructTriggerInstance(triggerInformation.getClassName(), currentActiveClassLoader);
      // get FailureStrategy
      triggerInformation.setFailureStrategy(trigger.getFailureStrategy());
      // register in trigger-table
      triggerTable.addTriggerInformation(triggerName, triggerInformation);
      // update PatternTreeMap
      patternTreeMap.append(triggerInformation.getPathPattern(), triggerName);
      // if it is a stateful trigger, we only maintain its instance on specified DataNode
      if (!triggerInformation.isStateful()
          || triggerInformation.getDataNodeLocation().getDataNodeId() == DATA_NODE_ID) {
        // construct and save TriggerExecutor after successfully creating trigger instance
        TriggerExecutor triggerExecutor = new TriggerExecutor(triggerInformation, trigger);
        executorMap.put(triggerName, triggerExecutor);
      }
    } catch (Exception e) {
      String errorMessage =
          String.format(
              "Failed to register trigger %s with className: %s. The cause is: %s",
              triggerInformation.getTriggerName(),
              triggerInformation.getClassName(),
              e.getMessage());
      LOGGER.warn(errorMessage);
      throw new TriggerManagementException(errorMessage);
    }
  }

  private Trigger constructTriggerInstance(String className, TriggerClassLoader classLoader)
      throws TriggerManagementException {
    try {
      Class<?> triggerClass = Class.forName(className, true, classLoader);
      return (Trigger) triggerClass.getDeclaredConstructor().newInstance();
    } catch (InstantiationException
        | InvocationTargetException
        | NoSuchMethodException
        | IllegalAccessException
        | ClassNotFoundException e) {
      throw new TriggerManagementException(
          String.format(
              "Failed to reflect trigger instance with className(%s), because %s", className, e));
    }
  }

  /**
   * @param triggerName given trigger
   * @return InternalRPC TEndPoint of DataNode where instance of given stateful trigger is on.
   */
  public TEndPoint getEndPointForStatefulTrigger(String triggerName) {
    TriggerInformation triggerInformation = triggerTable.getTriggerInformation(triggerName);
    if (triggerInformation.isStateful()) {
      return triggerInformation.getDataNodeLocation().getInternalEndPoint();
    }
    return null;
  }

  @Override
  public void start() throws StartupException {}

  @Override
  public void stop() {
    // nothing to do
  }

  @Override
  public ServiceType getID() {
    return ServiceType.TRIGGER_REGISTRATION_SERVICE;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // singleton instance holder
  /////////////////////////////////////////////////////////////////////////////////////////////////

  private static TriggerManagementService INSTANCE = null;

  public static synchronized TriggerManagementService setupAndGetInstance() {
    if (INSTANCE == null) {
      INSTANCE = new TriggerManagementService();
    }
    return INSTANCE;
  }

  public static TriggerManagementService getInstance() {
    return INSTANCE;
  }
}
