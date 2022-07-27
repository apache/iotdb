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

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.trigger.ITriggerRegistrationService;
import org.apache.iotdb.commons.trigger.TriggerClassLoader;
import org.apache.iotdb.commons.trigger.TriggerClassLoaderManager;
import org.apache.iotdb.commons.trigger.TriggerEvent;
import org.apache.iotdb.commons.trigger.TriggerOperationType;
import org.apache.iotdb.commons.trigger.TriggerRegistrationInformation;
import org.apache.iotdb.commons.trigger.api.Trigger;
import org.apache.iotdb.commons.trigger.exception.TriggerExecutionException;
import org.apache.iotdb.commons.trigger.exception.TriggerManagementException;
import org.apache.iotdb.commons.trigger.utils.TriggerLogReader;
import org.apache.iotdb.commons.trigger.utils.TriggerLogWriter;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.engine.trigger.executor.TriggerExecutor;
import org.apache.iotdb.db.metadata.idtable.IDTable;
import org.apache.iotdb.db.metadata.idtable.IDTableManager;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.query.dataset.ListDataSet;
import org.apache.iotdb.db.service.IoTDB;
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

import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_TRIGGER_ATTRIBUTES;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_TRIGGER_CLASS;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_TRIGGER_EVENT;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_TRIGGER_NAME;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_TRIGGER_PATH;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_TRIGGER_STATUS;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_TRIGGER_STATUS_STARTED;
import static org.apache.iotdb.commons.conf.IoTDBConstant.COLUMN_TRIGGER_STATUS_STOPPED;

public class TriggerRegistrationService implements ITriggerRegistrationService {

  private static final Logger LOGGER = LoggerFactory.getLogger(TriggerRegistrationService.class);

  private final String logFileDir;
  private final String logFileName;
  private final int tLogBufferSize;
  private final String temporaryLogFileName;
  private final String libRoot;
  private final boolean enableIDTable;
  private final ConcurrentHashMap<String, TriggerExecutor> executors;

  private TriggerLogWriter logWriter;

  private TriggerRegistrationService(
      String systemDir, String libRoot, int tLogBufferSize, boolean enableIDTable) {
    logFileDir = systemDir + File.separator + "trigger" + File.separator;
    logFileName = logFileDir + "tlog.bin";
    temporaryLogFileName = logFileName + ".tmp";
    this.tLogBufferSize = tLogBufferSize;
    this.libRoot = libRoot;
    this.enableIDTable = enableIDTable;
    executors = new ConcurrentHashMap<>();
  }

  @Override
  public synchronized void register(
      String triggerName,
      TriggerEvent event,
      PartialPath fullPath,
      String classPath,
      Map<String, String> attributes)
      throws TriggerManagementException, TriggerExecutionException {
    IMNode imNode = tryGetMNode(fullPath);
    checkIfRegistered(triggerName, classPath, imNode);
    TriggerRegistrationInformation registrationInformation =
        TriggerRegistrationInformation.getCreateInfo(
            triggerName, event, fullPath, classPath, attributes);
    tryAppendRegistrationLog(registrationInformation);
    doRegister(registrationInformation, imNode);
  }

  private IMNode tryGetMNode(PartialPath fullPath) throws TriggerManagementException {
    try {
      IMNode imNode = IoTDB.schemaProcessor.getMNodeForTrigger(fullPath);
      if (imNode == null) {
        throw new TriggerManagementException(
            String.format("Path [%s] does not exist", fullPath.getFullPath()));
      }
      return imNode;
    } catch (MetadataException e) {
      throw new RuntimeException(e);
    }
  }

  private void checkIfRegistered(String triggerName, String className, IMNode imNode)
      throws TriggerManagementException {
    TriggerExecutor executor = imNode.getTriggerExecutor();
    if (executor != null) {
      TriggerRegistrationInformation information = executor.getRegistrationInformation();
      throw new TriggerManagementException(
          String.format(
              "Failed to register trigger %s(%s), because a trigger %s(%s) has already been registered on the timeseries %s.",
              triggerName,
              className,
              information.getTriggerName(),
              information.getClassName(),
              imNode.getFullPath()));
    }

    executor = executors.get(triggerName);
    if (executor != null) {
      TriggerRegistrationInformation information = executor.getRegistrationInformation();
      throw new TriggerManagementException(
          information.getClassName().equals(className)
              ? String.format(
                  "Failed to register trigger %s(%s), because a trigger with the same trigger name and the class name has already been registered.",
                  triggerName, className)
              : String.format(
                  "Failed to register trigger %s(%s), because a trigger %s(%s) with the same trigger name but a different class name has already been registered.",
                  triggerName,
                  className,
                  information.getTriggerName(),
                  information.getClassName()));
    }
  }

  private void tryAppendRegistrationLog(TriggerRegistrationInformation registrationInformation)
      throws TriggerManagementException {
    try {
      logWriter.write(registrationInformation);
    } catch (IOException e) {
      throw new TriggerManagementException(
          String.format(
              "Failed to append trigger management operation log when registering trigger %s(%s), because %s",
              registrationInformation.getTriggerName(), registrationInformation.getClassName(), e));
    }
  }

  private void doRegister(TriggerRegistrationInformation registrationInformation, IMNode imNode)
      throws TriggerManagementException, TriggerExecutionException {
    TriggerClassLoader classLoader =
        TriggerClassLoaderManager.setUpAndGetInstance(libRoot)
            .register(registrationInformation.getClassName());

    TriggerExecutor executor;
    try {
      executor = new TriggerExecutor(registrationInformation, classLoader, imNode);
      executor.onCreate();
    } catch (TriggerManagementException | TriggerExecutionException e) {
      TriggerClassLoaderManager.setUpAndGetInstance(libRoot)
          .deregister(registrationInformation.getClassName());
      throw e;
    }

    executors.put(registrationInformation.getTriggerName(), executor);
    imNode.setTriggerExecutor(executor);

    // update id table
    if (enableIDTable) {
      try {
        IDTable idTable =
            IDTableManager.getInstance()
                .getIDTable(registrationInformation.getFullPath().getDevicePath());
        if (executor.getIMNode().isMeasurement()) {
          idTable.registerTrigger(
              registrationInformation.getFullPath(), (IMeasurementMNode) imNode);
        }
      } catch (MetadataException e) {
        throw new TriggerManagementException(e.getMessage(), e);
      }
    }
  }

  @Override
  public synchronized void deregister(String triggerName) throws TriggerManagementException {
    getTriggerExecutorWithExistenceCheck(triggerName);
    tryAppendDeregistrationLog(TriggerRegistrationInformation.getDropInfo(triggerName));
    doDeregister(triggerName);
  }

  private TriggerExecutor getTriggerExecutorWithExistenceCheck(String triggerName)
      throws TriggerManagementException {
    TriggerExecutor executor = executors.get(triggerName);

    if (executor == null) {
      throw new TriggerManagementException(
          String.format("Trigger %s does not exist.", triggerName));
    }

    return executor;
  }

  private void tryAppendDeregistrationLog(TriggerRegistrationInformation registrationInformation)
      throws TriggerManagementException {
    try {
      logWriter.write(registrationInformation);
    } catch (IOException e) {
      throw new TriggerManagementException(
          String.format(
              "Failed to drop trigger %s because the operation plan was failed to log: %s",
              registrationInformation.getTriggerName(), e));
    }
  }

  private void doDeregister(String triggerName) throws TriggerManagementException {
    TriggerExecutor executor = executors.remove(triggerName);

    IMNode imNode = executor.getIMNode();
    try {
      imNode.setTriggerExecutor(null);
      IoTDB.schemaProcessor.releaseMNodeAfterDropTrigger(imNode);
    } catch (MetadataException e) {
      throw new TriggerManagementException(e.getMessage(), e);
    }

    try {
      executor.onDrop();
    } catch (TriggerExecutionException e) {
      LOGGER.warn(e.getMessage(), e);
    }

    TriggerClassLoaderManager.setUpAndGetInstance(libRoot)
        .deregister(executor.getRegistrationInformation().getClassName());

    // update id table
    if (enableIDTable) {
      try {
        PartialPath fullPath = executor.getIMNode().getPartialPath();
        IDTable idTable = IDTableManager.getInstance().getIDTable(fullPath.getDevicePath());
        if (executor.getIMNode().isMeasurement()) {
          idTable.deregisterTrigger(fullPath, (IMeasurementMNode) executor.getIMNode());
        }
      } catch (MetadataException e) {
        throw new TriggerManagementException(e.getMessage(), e);
      }
    }
  }

  @Override
  public void activate(String triggerName)
      throws TriggerManagementException, TriggerExecutionException {
    TriggerExecutor executor = getTriggerExecutorWithExistenceCheck(triggerName);

    if (!executor.getRegistrationInformation().isStopped()) {
      throw new TriggerManagementException(
          String.format("Trigger %s has already been started.", triggerName));
    }

    try {
      logWriter.write(TriggerRegistrationInformation.getStartInfo(triggerName));
    } catch (IOException e) {
      throw new TriggerManagementException(
          String.format(
              "Failed to append trigger management operation log when starting trigger %s, because %s",
              triggerName, e));
    }

    executor.onStart();
  }

  @Override
  public void inactivate(String triggerName) throws TriggerManagementException {
    TriggerExecutor executor = getTriggerExecutorWithExistenceCheck(triggerName);

    if (executor.getRegistrationInformation().isStopped()) {
      throw new TriggerManagementException(
          String.format("Trigger %s has already been stopped.", triggerName));
    }

    try {
      logWriter.write(TriggerRegistrationInformation.getStopInfo(triggerName));
    } catch (IOException e) {
      throw new TriggerManagementException(
          String.format(
              "Failed to append trigger management operation log when stopping trigger %s, because %s",
              triggerName, e));
    }

    try {
      executor.onStop();
    } catch (TriggerExecutionException e) {
      LOGGER.warn(
          "Failed to stop the executor of trigger {}({})",
          executor.getRegistrationInformation().getTriggerName(),
          executor.getRegistrationInformation().getClassName(),
          e);
    }
  }

  @Override
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
      makeDirIfNecessary(libRoot);
      makeDirIfNecessary(logFileDir);
      doRecovery();
      logWriter = new TriggerLogWriter(logFileName, tLogBufferSize);
    } catch (Exception e) {
      throw new StartupException(e);
    }
  }

  private static void makeDirIfNecessary(String dir) throws IOException {
    File file = SystemFileFactory.INSTANCE.getFile(dir);
    if (file.exists() && file.isDirectory()) {
      return;
    }
    FileUtils.forceMkdir(file);
  }

  private void doRecovery() throws IOException, TriggerManagementException {
    File temporaryLogFile = SystemFileFactory.INSTANCE.getFile(temporaryLogFileName);
    File logFile = SystemFileFactory.INSTANCE.getFile(logFileName);

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
    for (TriggerRegistrationInformation registrationInformation :
        recoverRegistrationInfos(logFile)) {
      try {
        if (TriggerOperationType.CREATE == registrationInformation.getOperationType()) {
          boolean stopped = registrationInformation.isStopped();
          doRegister(registrationInformation, tryGetMNode(registrationInformation.getFullPath()));
          if (stopped) {
            executors.get(registrationInformation.getTriggerName()).onStop();
          }
        }
      } catch (TriggerExecutionException | TriggerManagementException e) {
        LOGGER.error(
            "Failed to register the trigger {}({}) during recovering.",
            registrationInformation.getTriggerName(),
            registrationInformation.getClassName());
      }
    }
  }

  private Collection<TriggerRegistrationInformation> recoverRegistrationInfos(File logFile)
      throws IOException, TriggerManagementException {
    Map<String, TriggerRegistrationInformation> recoveredTriggerRegistrationInfo = new HashMap<>();

    try (TriggerLogReader reader = new TriggerLogReader(logFile)) {
      while (reader.hasNext()) {
        TriggerRegistrationInformation registrationInformation = reader.next();
        switch (registrationInformation.getOperationType()) {
          case CREATE:
            recoveredTriggerRegistrationInfo.put(
                registrationInformation.getTriggerName(), registrationInformation);
            break;
          case DROP:
            recoveredTriggerRegistrationInfo.remove(registrationInformation.getTriggerName());
            break;
          case START:
            registrationInformation =
                recoveredTriggerRegistrationInfo.get(registrationInformation.getTriggerName());
            registrationInformation.markAsStarted();
            break;
          case STOP:
            registrationInformation =
                recoveredTriggerRegistrationInfo.get(registrationInformation.getTriggerName());
            registrationInformation.markAsStopped();
            break;
          default:
            throw new TriggerManagementException(
                "Unrecognized trigger management operation plan is recovered.");
        }
      }
    }
    return recoveredTriggerRegistrationInfo.values();
  }

  @Override
  public void stop() {
    try {
      writeTemporaryLogFile();

      logWriter.close();
      logWriter.deleteLogFile();

      File temporaryLogFile = SystemFileFactory.INSTANCE.getFile(temporaryLogFileName);
      File logFile = SystemFileFactory.INSTANCE.getFile(logFileName);
      FSFactoryProducer.getFSFactory().moveFile(temporaryLogFile, logFile);
    } catch (IOException ignored) {
      // ignored
    }
  }

  private void writeTemporaryLogFile() throws IOException {
    try (TriggerLogWriter temporaryLogWriter =
        new TriggerLogWriter(temporaryLogFileName, tLogBufferSize)) {
      for (TriggerExecutor executor : executors.values()) {
        TriggerRegistrationInformation information = executor.getRegistrationInformation();
        temporaryLogWriter.write(information);
        if (information.isStopped()) {
          temporaryLogWriter.write(
              TriggerRegistrationInformation.getStopInfo(information.getTriggerName()));
        }
      }
    }
  }

  @TestOnly
  public void deregisterAll() throws TriggerManagementException {
    for (TriggerExecutor executor : executors.values()) {
      deregister(executor.getRegistrationInformation().getTriggerName());
    }
  }

  @TestOnly
  public Trigger getTriggerInstance(String triggerName) throws TriggerManagementException {
    return getTriggerExecutorWithExistenceCheck(triggerName).getTrigger();
  }

  @TestOnly
  public TriggerRegistrationInformation getRegistrationInformation(String triggerName)
      throws TriggerManagementException {
    return getTriggerExecutorWithExistenceCheck(triggerName).getRegistrationInformation();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.TRIGGER_REGISTRATION_SERVICE;
  }

  public int executorSize() {
    return executors.size();
  }

  private static TriggerRegistrationService INSTANCE = null;

  public static TriggerRegistrationService setUpAndGetInstance(
      String systemDir, String libRoot, int tLogBufferSize, boolean enableIDTable) {
    if (INSTANCE == null) {
      INSTANCE = new TriggerRegistrationService(systemDir, libRoot, tLogBufferSize, enableIDTable);
    }
    return INSTANCE;
  }

  public static TriggerRegistrationService getInstance() {
    return INSTANCE;
  }
}
