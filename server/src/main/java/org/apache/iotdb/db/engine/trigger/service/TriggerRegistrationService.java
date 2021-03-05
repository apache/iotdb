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

package org.apache.iotdb.db.engine.trigger.service;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.engine.trigger.executor.TriggerExecutor;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.TriggerExecutionException;
import org.apache.iotdb.db.exception.TriggerManagementException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTriggerPlan;
import org.apache.iotdb.db.qp.physical.sys.DropTriggerPlan;
import org.apache.iotdb.db.qp.physical.sys.StartTriggerPlan;
import org.apache.iotdb.db.qp.physical.sys.StopTriggerPlan;
import org.apache.iotdb.db.query.dataset.ListDataSet;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Binary;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TRIGGER_ATTRIBUTES;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TRIGGER_CLASS;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TRIGGER_EVENT;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TRIGGER_NAME;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TRIGGER_PATH;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TRIGGER_STATUS;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TRIGGER_STATUS_STARTED;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TRIGGER_STATUS_STOPPED;

public class TriggerRegistrationService implements IService {

  private static final Logger LOGGER = LoggerFactory.getLogger(TriggerRegistrationService.class);
  private static final String LOGGER_MESSAGE_FAILED_TO_STOP =
      "Failed to stop the executor of trigger {}({})";

  private static final String LOG_FILE_DIR =
      IoTDBDescriptor.getInstance().getConfig().getSystemDir()
          + File.separator
          + "trigger"
          + File.separator;
  private static final String LOG_FILE_NAME = LOG_FILE_DIR + "tlog.bin";
  private static final String TEMPORARY_LOG_FILE_NAME = LOG_FILE_NAME + ".tmp";

  private final ConcurrentHashMap<String, TriggerExecutor> executors;

  private TriggerClassLoader classLoader;

  private TriggerLogWriter logWriter;

  public TriggerRegistrationService() {
    executors = new ConcurrentHashMap<>();
  }

  public synchronized void register(CreateTriggerPlan plan)
      throws TriggerManagementException, TriggerExecutionException {
    checkIfRegistered(plan);
    MeasurementMNode measurementMNode = tryGetMeasurementMNode(plan);
    updateClassLoader();
    doRegister(plan, measurementMNode);
    tryAppendRegistrationLog(plan, measurementMNode);
  }

  private void checkIfRegistered(CreateTriggerPlan plan) throws TriggerManagementException {
    TriggerExecutor executor = executors.get(plan.getTriggerName());
    if (executor == null) {
      return;
    }

    TriggerRegistrationInformation information = executor.getRegistrationInformation();
    throw new TriggerManagementException(
        information.getClassName().equals(plan.getClassName())
            ? String.format(
                "Failed to register trigger %s(%s), because a trigger with the same trigger name and the class name has already been registered.",
                plan.getTriggerName(), plan.getClassName())
            : String.format(
                "Failed to register trigger %s(%s), because a trigger %s(%s) with the same trigger name but a different class name has already been registered.",
                plan.getTriggerName(),
                plan.getClassName(),
                information.getTriggerName(),
                information.getClassName()));
  }

  private MeasurementMNode tryGetMeasurementMNode(CreateTriggerPlan plan)
      throws TriggerManagementException {
    try {
      return (MeasurementMNode) IoTDB.metaManager.getNodeByPath(plan.getFullPath());
    } catch (MetadataException e) {
      throw new TriggerManagementException(e.getMessage(), e);
    } catch (ClassCastException e) {
      throw new TriggerManagementException(
          "Triggers can only be registered on MeasurementMNode.", e);
    }
  }

  private void updateClassLoader() throws TriggerManagementException {
    TriggerClassLoader newClassLoader;

    // 1. construct a new trigger class loader
    try {
      newClassLoader = new TriggerClassLoader();
    } catch (IOException e) {
      throw new TriggerManagementException("Failed to construct a new trigger class loader.", e);
    }

    // 2. update instances of registered triggers using the new trigger class loader
    Collection<TriggerExecutor> executorCollection = executors.values();
    try {
      for (TriggerExecutor executor : executorCollection) {
        executor.prepareTriggerUpdate(newClassLoader);
      }
    } catch (Exception e) {
      for (TriggerExecutor executor : executorCollection) {
        executor.abortTriggerUpdate();
      }
      throw new TriggerManagementException("Failed to update trigger instances.", e);
    }
    for (TriggerExecutor executor : executorCollection) {
      executor.commitTriggerUpdate(newClassLoader);
    }

    // 3. close and replace old trigger class loader
    try {
      classLoader.close();
    } catch (IOException e) {
      LOGGER.warn("Failed to close old trigger classloader.", e);
    }
    classLoader = newClassLoader;
  }

  private void doRegister(CreateTriggerPlan plan, MeasurementMNode measurementMNode)
      throws TriggerManagementException, TriggerExecutionException {
    TriggerRegistrationInformation information = new TriggerRegistrationInformation(plan);
    TriggerExecutor executor = new TriggerExecutor(information, measurementMNode, classLoader);
    executor.onStart();

    executors.put(plan.getTriggerName(), executor);
    measurementMNode.setTriggerExecutor(executor);
  }

  private void tryAppendRegistrationLog(CreateTriggerPlan plan, MeasurementMNode measurementMNode)
      throws TriggerManagementException {
    try {
      logWriter.write(plan);
    } catch (IOException e) {
      measurementMNode.setTriggerExecutor(null);
      TriggerExecutor executor = executors.remove(plan.getTriggerName());
      try {
        executor.onStop();
      } catch (TriggerExecutionException triggerExecutionException) {
        LOGGER.warn(LOGGER_MESSAGE_FAILED_TO_STOP, plan.getTriggerName(), plan.getClassName(), e);
      }

      throw new TriggerManagementException(
          String.format(
              "Failed to append trigger management operation log when registering trigger %s(%s), because %s",
              plan.getTriggerName(), plan.getClassName(), e));
    }
  }

  public synchronized void deregister(DropTriggerPlan plan) throws TriggerManagementException {
    TriggerExecutor executor = tryRemoveAndStopExecutor(plan);
    tryAppendDeregistrationLog(plan, executor);
    executor.getMeasurementMNode().setTriggerExecutor(null);
  }

  private TriggerExecutor tryRemoveAndStopExecutor(DropTriggerPlan plan)
      throws TriggerManagementException {
    TriggerExecutor executor = executors.remove(plan.getTriggerName());

    if (executor == null) {
      throw new TriggerManagementException(
          String.format("Trigger %s does not exist.", plan.getTriggerName()));
    }

    try {
      executor.onStop();
    } catch (TriggerExecutionException e) {
      LOGGER.warn(
          LOGGER_MESSAGE_FAILED_TO_STOP,
          executor.getRegistrationInformation().getTriggerName(),
          executor.getRegistrationInformation().getClassName(),
          e);
    }

    return executor;
  }

  private void tryAppendDeregistrationLog(DropTriggerPlan plan, TriggerExecutor executor)
      throws TriggerManagementException {
    try {
      logWriter.write(plan);
    } catch (IOException e) {
      try {
        executor.onStart();
        executors.put(plan.getTriggerName(), executor);
      } catch (TriggerExecutionException triggerExecutionException) {
        executor.getMeasurementMNode().setTriggerExecutor(null);

        throw new TriggerManagementException(
            String.format(
                "Trigger %s(%s) is stopped. Failed to restart the trigger executor while logging the DropTriggerPlan failed, because %s",
                executor.getRegistrationInformation().getTriggerName(),
                executor.getRegistrationInformation().getClassName(),
                e));
      }

      throw new TriggerManagementException(
          String.format(
              "Trigger %s(%s) is not stopped. Failed to append trigger management operation log when deregistering the trigger, because %s",
              executor.getRegistrationInformation().getTriggerName(),
              executor.getRegistrationInformation().getClassName(),
              e));
    }
  }

  public void activate(StartTriggerPlan plan)
      throws TriggerManagementException, TriggerExecutionException {
    TriggerExecutor executor = executors.get(plan.getTriggerName());

    if (!executor.getRegistrationInformation().isStopped()) {
      throw new TriggerManagementException(
          String.format("Trigger %s has already been started.", plan.getTriggerName()));
    }

    try {
      logWriter.write(plan);
    } catch (IOException e) {
      throw new TriggerManagementException(
          String.format(
              "Failed to append trigger management operation log when starting trigger %s, because %s",
              plan.getTriggerName(), e));
    }

    executor.onStart();
  }

  public void inactivate(StopTriggerPlan plan) throws TriggerManagementException {
    TriggerExecutor executor = executors.get(plan.getTriggerName());

    if (executor == null) {
      throw new TriggerManagementException(
          String.format("Trigger %s does not exist.", plan.getTriggerName()));
    }

    if (executor.getRegistrationInformation().isStopped()) {
      throw new TriggerManagementException(
          String.format("Trigger %s has already been stopped.", plan.getTriggerName()));
    }

    try {
      logWriter.write(plan);
    } catch (IOException e) {
      throw new TriggerManagementException(
          String.format(
              "Failed to append trigger management operation log when stopping trigger %s, because %s",
              plan.getTriggerName(), e));
    }

    try {
      executor.onStop();
    } catch (TriggerExecutionException e) {
      LOGGER.warn(
          LOGGER_MESSAGE_FAILED_TO_STOP,
          executor.getRegistrationInformation().getTriggerName(),
          executor.getRegistrationInformation().getClassName(),
          e);
    }
  }

  public QueryDataSet show() {
    ListDataSet dataSet =
        new ListDataSet(
            Arrays.asList(
                new PartialPath(COLUMN_TRIGGER_NAME, false),
                new PartialPath(COLUMN_TRIGGER_STATUS, false),
                new PartialPath(COLUMN_TRIGGER_EVENT, false),
                new PartialPath(COLUMN_TRIGGER_PATH, false),
                new PartialPath(COLUMN_TRIGGER_CLASS, false),
                new PartialPath(COLUMN_TRIGGER_ATTRIBUTES, false)),
            Arrays.asList(
                TSDataType.TEXT,
                TSDataType.TEXT,
                TSDataType.TEXT,
                TSDataType.TEXT,
                TSDataType.TEXT,
                TSDataType.TEXT));
    putTriggerRecords(dataSet);
    return dataSet;
  }

  private void putTriggerRecords(ListDataSet dataSet) {
    for (TriggerExecutor executor : executors.values().toArray(new TriggerExecutor[0])) {
      TriggerRegistrationInformation information = executor.getRegistrationInformation();

      RowRecord rowRecord = new RowRecord(0); // ignore timestamp
      rowRecord.addField(Binary.valueOf(information.getTriggerName()), TSDataType.TEXT);
      rowRecord.addField(
          Binary.valueOf(
              information.isStopped()
                  ? COLUMN_TRIGGER_STATUS_STOPPED
                  : COLUMN_TRIGGER_STATUS_STARTED),
          TSDataType.TEXT);
      rowRecord.addField(Binary.valueOf(information.getEvent().toString()), TSDataType.TEXT);
      rowRecord.addField(Binary.valueOf(information.getFullPath().getFullPath()), TSDataType.TEXT);
      rowRecord.addField(Binary.valueOf(information.getClassName()), TSDataType.TEXT);
      rowRecord.addField(Binary.valueOf(information.getAttributes().toString()), TSDataType.TEXT);
      dataSet.putRecord(rowRecord);
    }
  }

  @Override
  public void start() throws StartupException {
    try {
      classLoader = new TriggerClassLoader();

      makeDirIfNecessary();
      doRecovery();
      logWriter = new TriggerLogWriter(LOG_FILE_NAME);
    } catch (Exception e) {
      throw new StartupException(e);
    }
  }

  private void makeDirIfNecessary() throws IOException {
    File file = SystemFileFactory.INSTANCE.getFile(LOG_FILE_DIR);
    if (file.exists() && file.isDirectory()) {
      return;
    }
    FileUtils.forceMkdir(file);
  }

  private void doRecovery() throws IOException, TriggerManagementException {
    File temporaryLogFile = SystemFileFactory.INSTANCE.getFile(TEMPORARY_LOG_FILE_NAME);
    File logFile = SystemFileFactory.INSTANCE.getFile(LOG_FILE_NAME);

    if (temporaryLogFile.exists()) {
      if (logFile.exists()) {
        doRecoveryFromLogFile(logFile);
        FileUtils.deleteQuietly(temporaryLogFile);
      } else {
        doRecoveryFromLogFile(temporaryLogFile);
        FSFactoryProducer.getFSFactory().moveFile(temporaryLogFile, logFile);
      }
    } else if (logFile.exists()) {
      doRecoveryFromLogFile(logFile);
    }
  }

  private void doRecoveryFromLogFile(File logFile) throws IOException, TriggerManagementException {
    for (CreateTriggerPlan createTriggerPlan : recoverCreateTriggerPlans(logFile)) {
      try {
        doRegister(createTriggerPlan, tryGetMeasurementMNode(createTriggerPlan));
      } catch (TriggerExecutionException | TriggerManagementException e) {
        LOGGER.error(
            "Failed to register the trigger {}({}) during recovering.",
            createTriggerPlan.getTriggerName(),
            createTriggerPlan.getClassName());
      }
    }
  }

  private Collection<CreateTriggerPlan> recoverCreateTriggerPlans(File logFile)
      throws IOException, TriggerManagementException {
    Map<String, CreateTriggerPlan> recoveredCreateTriggerPlans = new HashMap<>();

    try (TriggerLogReader reader = new TriggerLogReader(logFile)) {
      while (reader.hasNext()) {
        PhysicalPlan plan = reader.next();
        CreateTriggerPlan createTriggerPlan;
        switch (plan.getOperatorType()) {
          case CREATE_TRIGGER:
            recoveredCreateTriggerPlans.put(
                ((CreateTriggerPlan) plan).getTriggerName(), (CreateTriggerPlan) plan);
            break;
          case DROP_TRIGGER:
            recoveredCreateTriggerPlans.remove(((DropTriggerPlan) plan).getTriggerName());
            break;
          case START_TRIGGER:
            createTriggerPlan =
                recoveredCreateTriggerPlans.get(((StartTriggerPlan) plan).getTriggerName());
            if (createTriggerPlan != null) {
              createTriggerPlan.markAsStarted();
            }
            break;
          case STOP_TRIGGER:
            createTriggerPlan =
                recoveredCreateTriggerPlans.get(((StopTriggerPlan) plan).getTriggerName());
            if (createTriggerPlan != null) {
              createTriggerPlan.markAsStopped();
            }
            break;
          default:
            throw new TriggerManagementException(
                "Unrecognized trigger management operation plan is recovered.");
        }
      }
    }

    return recoveredCreateTriggerPlans.values();
  }

  @Override
  public void stop() {
    try {
      writeTemporaryLogFile();

      logWriter.close();
      logWriter.deleteLogFile();

      File temporaryLogFile = SystemFileFactory.INSTANCE.getFile(TEMPORARY_LOG_FILE_NAME);
      File logFile = SystemFileFactory.INSTANCE.getFile(LOG_FILE_NAME);
      FSFactoryProducer.getFSFactory().moveFile(temporaryLogFile, logFile);
    } catch (IOException ignored) {
      // ignored
    }
  }

  private void writeTemporaryLogFile() throws IOException {
    try (TriggerLogWriter temporaryLogWriter = new TriggerLogWriter(TEMPORARY_LOG_FILE_NAME)) {
      for (TriggerExecutor executor : executors.values()) {
        TriggerRegistrationInformation information = executor.getRegistrationInformation();
        temporaryLogWriter.write(information.convertToCreateTriggerPlan());
        if (information.isStopped()) {
          temporaryLogWriter.write(new StopTriggerPlan(information.getTriggerName()));
        }
      }
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.TRIGGER_REGISTRATION_SERVICE;
  }

  public static TriggerRegistrationService getInstance() {
    return TriggerRegistrationService.TriggerRegistrationServiceHelper.INSTANCE;
  }

  private static class TriggerRegistrationServiceHelper {

    private static final TriggerRegistrationService INSTANCE = new TriggerRegistrationService();

    private TriggerRegistrationServiceHelper() {}
  }
}
