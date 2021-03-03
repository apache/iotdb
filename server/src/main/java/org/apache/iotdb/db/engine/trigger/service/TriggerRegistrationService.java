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
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.TriggerManagementException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTriggerPlan;
import org.apache.iotdb.db.qp.physical.sys.DropTriggerPlan;
import org.apache.iotdb.db.qp.physical.sys.StartTriggerPlan;
import org.apache.iotdb.db.qp.physical.sys.StopTriggerPlan;
import org.apache.iotdb.db.query.dataset.ListDataSet;
import org.apache.iotdb.db.service.IService;
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
  private static final String LOGGER_MESSAGE_TRIGGER_NOT_EXISTED = "Trigger %s does not exist.";

  private static final String LOG_FILE_DIR =
      IoTDBDescriptor.getInstance().getConfig().getSystemDir()
          + File.separator
          + "trigger"
          + File.separator;
  private static final String LOG_FILE_NAME = LOG_FILE_DIR + "tlog.bin";
  private static final String TEMPORARY_LOG_FILE_NAME = LOG_FILE_NAME + ".tmp";

  private final ConcurrentHashMap<String, TriggerRegistrationInformation> registrationInformation;

  private TriggerLogWriter logWriter;

  public TriggerRegistrationService() {
    registrationInformation = new ConcurrentHashMap<>();
  }

  public synchronized void register(CreateTriggerPlan plan) throws TriggerManagementException {
    checkIfRegistered(plan);
    doRegister(plan);
    tryAppendRegistrationLog(plan);
  }

  private void checkIfRegistered(CreateTriggerPlan plan) throws TriggerManagementException {
    TriggerRegistrationInformation information = registrationInformation.get(plan.getTriggerName());
    if (information == null) {
      return;
    }

    String errorMessage =
        information.getClassName().equals(plan.getClassName())
            ? String.format(
                "Failed to register trigger %s(%s), because a trigger with the same trigger name and the class name has already been registered.",
                plan.getTriggerName(), plan.getClassName())
            : String.format(
                "Failed to register trigger %s(%s), because a trigger %s(%s) with the same trigger name but a different class name has already been registered.",
                plan.getTriggerName(),
                plan.getClassName(),
                information.getTriggerName(),
                information.getClassName());
    LOGGER.warn(errorMessage);
    throw new TriggerManagementException(errorMessage);
  }

  private void doRegister(CreateTriggerPlan plan) {
    // TODO:
    // 1. update class loader and related classes
    // 2. try to construct instance
    // 3. create and put the trigger executor to MTree
    // 4. put the registration information into registrationInformation
  }

  private void tryAppendRegistrationLog(CreateTriggerPlan plan) throws TriggerManagementException {
    try {
      logWriter.write(plan);
    } catch (IOException e) {
      registrationInformation.remove(plan.getTriggerName());
      String errorMessage =
          String.format(
              "Failed to append trigger management operation log when registering trigger %s(%s), because %s",
              plan.getTriggerName(), plan.getClassName(), e);
      LOGGER.error(errorMessage);
      throw new TriggerManagementException(errorMessage, e);
    }
  }

  public synchronized void deregister(DropTriggerPlan plan) throws TriggerManagementException {
    TriggerRegistrationInformation information = tryRemoveRegistrationInformation(plan);
    doDeregister(plan, information);
    tryAppendDeregistrationLog(plan, information);
  }

  private TriggerRegistrationInformation tryRemoveRegistrationInformation(DropTriggerPlan plan)
      throws TriggerManagementException {
    TriggerRegistrationInformation information =
        registrationInformation.remove(plan.getTriggerName());

    if (information == null) {
      String errorMessage =
          String.format(LOGGER_MESSAGE_TRIGGER_NOT_EXISTED, plan.getTriggerName());
      LOGGER.warn(errorMessage);
      throw new TriggerManagementException(errorMessage);
    }

    return information;
  }

  private void doDeregister(DropTriggerPlan plan, TriggerRegistrationInformation information)
      throws TriggerManagementException {
    try {
      // TODO: remove the trigger executor from MTree
    } catch (Exception e) {
      registrationInformation.put(plan.getTriggerName(), information);
      String errorMessage =
          String.format(
              "Failed to remove the trigger executor from MTree when deregistering trigger %s, because %s",
              plan.getTriggerName(), e);
      LOGGER.error(errorMessage);
      throw new TriggerManagementException(errorMessage, e);
    }
  }

  private void tryAppendDeregistrationLog(
      DropTriggerPlan plan, TriggerRegistrationInformation information)
      throws TriggerManagementException {
    try {
      logWriter.write(plan);
    } catch (IOException e) {
      registrationInformation.put(plan.getTriggerName(), information);
      String errorMessage =
          String.format(
              "Failed to append trigger management operation log when deregistering trigger %s, because %s",
              plan.getTriggerName(), e);
      LOGGER.error(errorMessage);
      throw new TriggerManagementException(errorMessage, e);
    }
  }

  public void activate(StartTriggerPlan plan) throws TriggerManagementException {
    TriggerRegistrationInformation information = registrationInformation.get(plan.getTriggerName());
    if (information == null) {
      String errorMessage =
          String.format(LOGGER_MESSAGE_TRIGGER_NOT_EXISTED, plan.getTriggerName());
      LOGGER.warn(errorMessage);
      throw new TriggerManagementException(errorMessage);
    }

    if (!information.isStopped()) {
      String errorMessage =
          String.format("Trigger %s has already been started.", plan.getTriggerName());
      LOGGER.warn(errorMessage);
      throw new TriggerManagementException(errorMessage);
    }

    try {
      logWriter.write(plan);
    } catch (IOException e) {
      String errorMessage =
          String.format(
              "Failed to append trigger management operation log when starting trigger %s, because %s",
              plan.getTriggerName(), e);
      LOGGER.error(errorMessage);
      throw new TriggerManagementException(errorMessage, e);
    }

    information.markAsStarted();
  }

  public void inactivate(StopTriggerPlan plan) throws TriggerManagementException {
    TriggerRegistrationInformation information = registrationInformation.get(plan.getTriggerName());
    if (information == null) {
      String errorMessage =
          String.format(LOGGER_MESSAGE_TRIGGER_NOT_EXISTED, plan.getTriggerName());
      LOGGER.warn(errorMessage);
      throw new TriggerManagementException(errorMessage);
    }

    if (information.isStopped()) {
      String errorMessage =
          String.format("Trigger %s has already been stopped.", plan.getTriggerName());
      LOGGER.warn(errorMessage);
      throw new TriggerManagementException(errorMessage);
    }

    try {
      logWriter.write(plan);
    } catch (IOException e) {
      String errorMessage =
          String.format(
              "Failed to append trigger management operation log when stopping trigger %s, because %s",
              plan.getTriggerName(), e);
      LOGGER.error(errorMessage);
      throw new TriggerManagementException(errorMessage, e);
    }

    information.markAsStopped();
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
    for (TriggerRegistrationInformation info : getRegistrationInformation()) {
      RowRecord rowRecord = new RowRecord(0); // ignore timestamp
      rowRecord.addField(Binary.valueOf(info.getTriggerName()), TSDataType.TEXT);
      rowRecord.addField(
          Binary.valueOf(
              info.isStopped() ? COLUMN_TRIGGER_STATUS_STOPPED : COLUMN_TRIGGER_STATUS_STARTED),
          TSDataType.TEXT);
      rowRecord.addField(Binary.valueOf(info.getEvent().toString()), TSDataType.TEXT);
      rowRecord.addField(Binary.valueOf(info.getFullPath().getFullPath()), TSDataType.TEXT);
      rowRecord.addField(Binary.valueOf(info.getClassName()), TSDataType.TEXT);
      rowRecord.addField(Binary.valueOf(info.getAttributes().toString()), TSDataType.TEXT);
      dataSet.putRecord(rowRecord);
    }
  }

  private TriggerRegistrationInformation[] getRegistrationInformation() {
    return registrationInformation.values().toArray(new TriggerRegistrationInformation[0]);
  }

  @Override
  public void start() throws StartupException {
    try {
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
      doRegister(createTriggerPlan); // trigger status is also be recovered
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
      for (TriggerRegistrationInformation information : registrationInformation.values()) {
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
