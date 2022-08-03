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

package org.apache.iotdb.commons.trigger;

import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.trigger.exception.TriggerExecutionException;
import org.apache.iotdb.commons.trigger.exception.TriggerManagementException;
import org.apache.iotdb.commons.trigger.utils.TriggerLogReader;
import org.apache.iotdb.commons.trigger.utils.TriggerLogWriter;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** BaseTriggerRegistrationService just provides trigger log service without trigger executing */
public class BaseTriggerRegistrationService implements ITriggerRegistrationService {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(BaseTriggerRegistrationService.class);

  private final String logFileDir;
  private final String logFileName;
  private final int tLogBufferSize;
  private final String temporaryLogFileName;
  private final String libRoot;
  private TriggerLogWriter logWriter;

  private final Map<String, TriggerRegistrationInformation> triggerMap;

  private BaseTriggerRegistrationService(String systemDir, String libRoot, int tLogBufferSize) {
    logFileDir = systemDir + File.separator + "trigger" + File.separator;
    logFileName = logFileDir + "tlog.bin";
    temporaryLogFileName = logFileName + ".tmp";
    this.tLogBufferSize = tLogBufferSize;
    this.libRoot = libRoot;
    triggerMap = new ConcurrentHashMap<>();
  }

  @Override
  public synchronized void register(
      String triggerName,
      TriggerEvent event,
      PartialPath fullPath,
      String className,
      Map<String, String> attributes)
      throws TriggerManagementException, TriggerExecutionException {
    checkIfRegistered(triggerName);
    TriggerRegistrationInformation registrationInformation =
        TriggerRegistrationInformation.getCreateInfo(
            triggerName, event, fullPath, className, attributes);
    tryAppendRegistrationLog(registrationInformation);
    doRegister(registrationInformation);
  }

  private void checkIfRegistered(String triggerName) throws TriggerManagementException {
    if (triggerMap.containsKey(triggerName)) {
      throw new TriggerManagementException(
          String.format(
              "Failed to register trigger %s, because a trigger %s has already been registered.",
              triggerName, triggerName));
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

  private void doRegister(TriggerRegistrationInformation registrationInformation)
      throws TriggerManagementException, TriggerExecutionException {
    TriggerClassLoader classLoader =
        TriggerClassLoaderManager.setUpAndGetInstance(libRoot)
            .register(registrationInformation.getClassName());
    try {
      Class.forName(registrationInformation.getClassName(), true, classLoader);
    } catch (ClassNotFoundException e) {
      throw new TriggerManagementException(
          String.format(
              "Failed to reflect Trigger %s(%s) instance, because %s",
              registrationInformation.getTriggerName(), registrationInformation.getClassName(), e));
    }
    triggerMap.put(registrationInformation.getTriggerName(), registrationInformation);
  }

  @Override
  public synchronized void deregister(String triggerName) throws TriggerManagementException {
    getRegistrationInfoWithExistenceCheck(triggerName);
    tryAppendDeregistrationLog(TriggerRegistrationInformation.getDropInfo(triggerName));
    doDeregister(triggerName);
  }

  private TriggerRegistrationInformation getRegistrationInfoWithExistenceCheck(String triggerName)
      throws TriggerManagementException {
    if (!triggerMap.containsKey(triggerName)) {
      throw new TriggerManagementException(
          String.format("Trigger %s does not exits.", triggerName));
    }
    return triggerMap.get(triggerName);
  }

  private void tryAppendDeregistrationLog(TriggerRegistrationInformation registrationInformation)
      throws TriggerManagementException {
    try {
      logWriter.write(registrationInformation);
    } catch (IOException e) {
      throw new TriggerManagementException(
          String.format(
              "Failed to drop trigger %s because the operation type was failed to log: %s",
              registrationInformation.getTriggerName(), e));
    }
  }

  private void doDeregister(String triggerName) {
    TriggerRegistrationInformation registrationInfo = triggerMap.remove(triggerName);
    TriggerClassLoaderManager.setUpAndGetInstance(libRoot)
        .deregister(registrationInfo.getClassName());
  }

  @Override
  public void activate(String triggerName) throws TriggerManagementException {
    TriggerRegistrationInformation registrationInfo =
        getRegistrationInfoWithExistenceCheck(triggerName);

    if (!registrationInfo.isStopped()) {
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
  }

  @Override
  public void inactivate(String triggerName) throws TriggerManagementException {
    TriggerRegistrationInformation registrationInfo =
        getRegistrationInfoWithExistenceCheck(triggerName);

    if (registrationInfo.isStopped()) {
      throw new TriggerManagementException(
          String.format("Trigger %s has already been started.", triggerName));
    }

    try {
      logWriter.write(TriggerRegistrationInformation.getStopInfo(triggerName));
    } catch (IOException e) {
      throw new TriggerManagementException(
          String.format(
              "Failed to append trigger management operation log when stopping trigger %s, because %s",
              triggerName, e));
    }
  }

  @Override
  public QueryDataSet show() {
    throw new UnsupportedOperationException("ConfigNode doesn't support SHOW TRIGGERS");
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
        recoverRegistrationInfo(logFile)) {
      try {
        if (TriggerManagementType.CREATE == registrationInformation.getManagementType()) {
          doRegister(registrationInformation);
        }
      } catch (TriggerExecutionException | TriggerManagementException e) {
        LOGGER.error(
            "Failed to register the trigger {}({}) during recovering.",
            registrationInformation.getTriggerName(),
            registrationInformation.getClassName());
      }
    }
  }

  private Collection<TriggerRegistrationInformation> recoverRegistrationInfo(File logFile)
      throws IOException, TriggerManagementException {
    Map<String, TriggerRegistrationInformation> recoveredTriggerRegistrationInfo = new HashMap<>();

    try (TriggerLogReader reader = new TriggerLogReader(logFile)) {
      while (reader.hasNext()) {
        TriggerRegistrationInformation registrationInformation = reader.next();
        switch (registrationInformation.getManagementType()) {
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
            if (registrationInformation != null) {
              registrationInformation.markAsStarted();
            }
            break;
          case STOP:
            registrationInformation =
                recoveredTriggerRegistrationInfo.get(registrationInformation.getTriggerName());
            if (registrationInformation != null) {
              registrationInformation.markAsStopped();
            }
            break;
          default:
            throw new TriggerManagementException(
                "Unrecognized trigger management operation type is recovered.");
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
      for (TriggerRegistrationInformation registrationInfo : triggerMap.values()) {
        temporaryLogWriter.write(registrationInfo);
        if (registrationInfo.isStopped()) {
          temporaryLogWriter.write(
              TriggerRegistrationInformation.getStopInfo(registrationInfo.getTriggerName()));
        }
      }
    }
  }

  private static BaseTriggerRegistrationService INSTANCE = null;

  public static synchronized BaseTriggerRegistrationService setUpAndGetInstance(
      String systemDir, String libRoot, int tLogBufferSize) {
    if (INSTANCE == null) {
      INSTANCE = new BaseTriggerRegistrationService(systemDir, libRoot, tLogBufferSize);
    }
    return INSTANCE;
  }

  public static BaseTriggerRegistrationService getInstance() {
    return INSTANCE;
  }
}
