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

package org.apache.iotdb.commons.udf.service;

import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.executable.ExecutableResource;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.commons.udf.builtin.BuiltinAggregationFunction;
import org.apache.iotdb.commons.udf.builtin.BuiltinTimeSeriesGeneratingFunction;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.udf.api.UDF;
import org.apache.iotdb.udf.api.exception.UDFRegistrationException;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class UDFRegistrationService implements IService, SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(UDFRegistrationService.class);

  private final String ulogFileDir;
  private final String logFileName;
  private final String temporaryLogFileName;

  private final ReentrantLock registrationLock;
  private ConcurrentHashMap<String, UDFRegistrationInformation> registrationInformation;

  private final ReentrantReadWriteLock logWriterLock;
  private UDFLogWriter logWriter;

  private UDFRegistrationService(String ulogFileDir) {
    this.ulogFileDir = ulogFileDir;
    logFileName = ulogFileDir + "ulog.txt";
    temporaryLogFileName = logFileName + ".tmp";

    registrationLock = new ReentrantLock();
    registrationInformation = new ConcurrentHashMap<>();
    logWriterLock = new ReentrantReadWriteLock();
  }

  public void acquireRegistrationLock() {
    registrationLock.lock();
  }

  public void releaseRegistrationLock() {
    registrationLock.unlock();
  }

  /** invoked by config leader for validation before registration */
  public void validate(String functionName, String className) {
    functionName = functionName.toUpperCase();
    validateFunctionName(functionName, className);
    checkIfRegistered(functionName, className);
  }

  public void register(String functionName, String className, boolean writeToTemporaryLogFile)
      throws UDFRegistrationException {
    functionName = functionName.toUpperCase();
    validateFunctionName(functionName, className);
    checkIfRegistered(functionName, className);
    doRegister(functionName, className, Collections.emptyList());
    tryAppendRegistrationLog(
        functionName, className, Collections.emptyList(), writeToTemporaryLogFile);
  }

  public void register(
      String functionName,
      String className,
      List<String> uris,
      UDFExecutableManager udfExecutableManager,
      boolean writeToTemporaryLogFile)
      throws UDFRegistrationException {
    functionName = functionName.toUpperCase();
    validateFunctionName(functionName, className);
    checkIfRegistered(functionName, className);
    downloadExecutableResources(functionName, className, uris, udfExecutableManager);
    doRegister(functionName, className, uris);
    tryAppendRegistrationLog(functionName, className, uris, writeToTemporaryLogFile);
  }

  private static void validateFunctionName(String functionName, String className)
      throws UDFRegistrationException {
    if (!BuiltinAggregationFunction.getNativeFunctionNames().contains(functionName.toLowerCase())) {
      return;
    }

    String errorMessage =
        String.format(
            "Failed to register UDF %s(%s), because the given function name conflicts with the built-in function name",
            functionName, className);

    LOGGER.warn(errorMessage);
    throw new UDFRegistrationException(errorMessage);
  }

  private void checkIfRegistered(String functionName, String className)
      throws UDFRegistrationException {
    UDFRegistrationInformation information = registrationInformation.get(functionName);
    if (information == null) {
      return;
    }

    String errorMessage;
    if (information.isBuiltin()) {
      errorMessage =
          String.format(
              "Failed to register UDF %s(%s), because the given function name is the same as a built-in UDF function name.",
              functionName, className);
    } else {
      if (information.getClassName().equals(className)) {
        errorMessage =
            String.format(
                "Failed to register UDF %s(%s), because a UDF %s(%s) with the same function name and the class name has already been registered.",
                functionName, className, information.getFunctionName(), information.getClassName());
      } else {
        errorMessage =
            String.format(
                "Failed to register UDF %s(%s), because a UDF %s(%s) with the same function name but a different class name has already been registered.",
                functionName, className, information.getFunctionName(), information.getClassName());
      }
    }

    LOGGER.warn(errorMessage);
    throw new UDFRegistrationException(errorMessage);
  }

  private void downloadExecutableResources(
      String functionName,
      String className,
      List<String> uris,
      UDFExecutableManager udfExecutableManager)
      throws UDFRegistrationException {
    if (uris.isEmpty()) {
      return;
    }

    try {
      final ExecutableResource resource = udfExecutableManager.request(uris);
      try {
        udfExecutableManager.removeFromExtLibDir(functionName);
        udfExecutableManager.moveToExtLibDir(resource, functionName);
      } catch (Exception innerException) {
        udfExecutableManager.removeFromExtLibDir(functionName);
        udfExecutableManager.removeFromTemporaryLibRoot(resource);
        throw innerException;
      }
    } catch (Exception outerException) {
      String errorMessage =
          String.format(
              "Failed to register UDF %s(%s) because failed to fetch UDF executables(%s)",
              functionName, className, uris);
      LOGGER.warn(errorMessage, outerException);
      throw new UDFRegistrationException(errorMessage, outerException);
    }
  }

  private void doRegister(String functionName, String className, List<String> uris)
      throws UDFRegistrationException {
    acquireRegistrationLock();
    try {
      UDFClassLoader currentActiveClassLoader =
          UDFClassLoaderManager.getInstance().updateAndGetActiveClassLoader();
      updateAllRegisteredClasses(currentActiveClassLoader);

      Class<?> functionClass = Class.forName(className, true, currentActiveClassLoader);
      functionClass.getDeclaredConstructor().newInstance();
      registrationInformation.put(
          functionName,
          new UDFRegistrationInformation(functionName, className, uris, false, functionClass));
    } catch (IOException
        | InstantiationException
        | InvocationTargetException
        | NoSuchMethodException
        | IllegalAccessException
        | ClassNotFoundException e) {
      String errorMessage =
          String.format(
              "Failed to register UDF %s(%s), because its instance can not be constructed successfully. Exception: %s",
              functionName, className, e);
      LOGGER.warn(errorMessage, e);
      throw new UDFRegistrationException(errorMessage);
    } finally {
      releaseRegistrationLock();
    }
  }

  private void tryAppendRegistrationLog(
      String functionName, String className, List<String> uris, boolean writeToTemporaryLogFile)
      throws UDFRegistrationException {
    if (!writeToTemporaryLogFile) {
      return;
    }

    try {
      appendRegistrationLog(functionName, className, uris);
    } catch (IOException e) {
      registrationInformation.remove(functionName);
      String errorMessage =
          String.format(
              "Failed to append UDF log when registering UDF %s(%s), because %s",
              functionName, className, e);
      LOGGER.error(errorMessage);
      throw new UDFRegistrationException(errorMessage, e);
    }
  }

  private void updateAllRegisteredClasses(UDFClassLoader activeClassLoader)
      throws ClassNotFoundException {
    for (UDFRegistrationInformation information : getRegistrationInformation()) {
      if (!information.isBuiltin()) {
        information.updateFunctionClass(activeClassLoader);
      }
    }
  }

  public void deregister(String functionName) throws UDFRegistrationException {
    functionName = functionName.toUpperCase();
    UDFRegistrationInformation information = registrationInformation.get(functionName);
    if (information == null) {
      String errorMessage = String.format("UDF %s does not exist.", functionName);
      LOGGER.warn(errorMessage);
      throw new UDFRegistrationException(errorMessage);
    }

    if (information.isBuiltin()) {
      String errorMessage =
          String.format("Built-in function %s can not be deregistered.", functionName);
      LOGGER.warn(errorMessage);
      throw new UDFRegistrationException(errorMessage);
    }

    try {
      appendDeregistrationLog(functionName);
      registrationInformation.remove(functionName);
    } catch (IOException e) {
      String errorMessage =
          String.format(
              "Failed to append UDF log when deregistering UDF %s, because %s", functionName, e);
      LOGGER.error(errorMessage);
      throw new UDFRegistrationException(errorMessage, e);
    }
  }

  private void appendRegistrationLog(String functionName, String className, List<String> uris)
      throws IOException {
    logWriterLock.writeLock().lock();
    try {
      logWriter.register(functionName, className, uris);
    } finally {
      logWriterLock.writeLock().unlock();
    }
  }

  private void appendDeregistrationLog(String functionName) throws IOException {
    logWriterLock.writeLock().lock();
    try {
      logWriter.deregister(functionName);
    } finally {
      logWriterLock.writeLock().unlock();
    }
  }

  public UDF reflect(String functionName) {
    functionName = functionName.toUpperCase();
    UDFRegistrationInformation information = registrationInformation.get(functionName);
    if (information == null) {
      String errorMessage =
          String.format(
              "Failed to reflect UDF instance, because UDF %s has not been registered.",
              functionName);
      LOGGER.warn(errorMessage);
      throw new RuntimeException(errorMessage);
    }

    if (!information.isBuiltin()) {
      Thread.currentThread()
          .setContextClassLoader(UDFClassLoaderManager.getInstance().getActiveClassLoader());
    }

    try {
      return (UDF) information.getFunctionClass().getDeclaredConstructor().newInstance();
    } catch (InstantiationException
        | InvocationTargetException
        | NoSuchMethodException
        | IllegalAccessException e) {
      String errorMessage =
          String.format(
              "Failed to reflect UDF %s(%s) instance, because %s",
              functionName, information.getClassName(), e);
      LOGGER.warn(errorMessage, e);
      throw new RuntimeException(errorMessage);
    }
  }

  public UDFRegistrationInformation[] getRegistrationInformation() {
    return registrationInformation.values().toArray(new UDFRegistrationInformation[0]);
  }

  @Override
  public void start() throws StartupException {
    try {
      recovery();
    } catch (Exception e) {
      throw new StartupException(e);
    }
  }

  private void recovery() throws Exception {
    registrationInformation = new ConcurrentHashMap<>();
    registerBuiltinTimeSeriesGeneratingFunctions();
    makeDirIfNecessary();
    doRecovery();
    logWriter = new UDFLogWriter(logFileName);
  }

  private void registerBuiltinTimeSeriesGeneratingFunctions() {
    for (BuiltinTimeSeriesGeneratingFunction builtinTimeSeriesGeneratingFunction :
        BuiltinTimeSeriesGeneratingFunction.values()) {
      String functionName = builtinTimeSeriesGeneratingFunction.getFunctionName();
      registrationInformation.put(
          functionName,
          new UDFRegistrationInformation(
              functionName,
              builtinTimeSeriesGeneratingFunction.getClassName(),
              Collections.emptyList(),
              true,
              builtinTimeSeriesGeneratingFunction.getFunctionClass()));
    }
  }

  private void makeDirIfNecessary() throws IOException {
    File file = SystemFileFactory.INSTANCE.getFile(ulogFileDir);
    if (file.exists() && file.isDirectory()) {
      return;
    }
    FileUtils.forceMkdir(file);
  }

  private void doRecovery() throws IOException {
    File temporaryLogFile = SystemFileFactory.INSTANCE.getFile(temporaryLogFileName);
    File logFile = SystemFileFactory.INSTANCE.getFile(logFileName);

    if (temporaryLogFile.exists()) {
      if (logFile.exists()) {
        recoveryFromLogFile(logFile);
        FileUtils.deleteQuietly(temporaryLogFile);
      } else {
        recoveryFromLogFile(temporaryLogFile);
        FSFactoryProducer.getFSFactory().moveFile(temporaryLogFile, logFile);
      }
    } else if (logFile.exists()) {
      recoveryFromLogFile(logFile);
    }
  }

  private void recoveryFromLogFile(File logFile) throws IOException {
    HashMap<String, String> recoveredUDFs = new HashMap<>();

    try (BufferedReader reader = new BufferedReader(new FileReader(logFile))) {
      String line;
      while ((line = reader.readLine()) != null) {
        String[] data = line.split(",");
        byte type = Byte.parseByte(data[0]);
        if (type == UDFLogWriter.REGISTER_WITHOUT_URIS_TYPE
            || type == UDFLogWriter.REGISTER_WITH_URIS_TYPE) {
          recoveredUDFs.put(data[1], data[2]);
        } else if (type == UDFLogWriter.DEREGISTER_TYPE) {
          recoveredUDFs.remove(data[1]);
        } else {
          throw new UnsupportedEncodingException();
        }
      }
    }

    for (Entry<String, String> udf : recoveredUDFs.entrySet()) {
      try {
        register(udf.getKey(), udf.getValue(), false);
      } catch (UDFRegistrationException ignored) {
        // ignored
      }
    }
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
    UDFLogWriter temporaryLogFile = new UDFLogWriter(temporaryLogFileName);
    for (UDFRegistrationInformation information : registrationInformation.values()) {
      if (information.isBuiltin()) {
        continue;
      }
      temporaryLogFile.register(
          information.getFunctionName(), information.getClassName(), information.getUris());
    }
    temporaryLogFile.close();
  }

  @TestOnly
  public void deregisterAll() throws UDFRegistrationException {
    for (UDFRegistrationInformation information : getRegistrationInformation()) {
      if (!information.isBuiltin()) {
        deregister(information.getFunctionName());
      }
    }
  }

  @TestOnly
  public void registerBuiltinFunction(String functionName, String className)
      throws ClassNotFoundException {
    ClassLoader classLoader = getClass().getClassLoader();
    Class<?> functionClass = Class.forName(className, true, classLoader);
    functionName = functionName.toUpperCase();
    registrationInformation.put(
        functionName,
        new UDFRegistrationInformation(
            functionName, className, Collections.emptyList(), true, functionClass));
  }

  @TestOnly
  public void deregisterBuiltinFunction(String functionName) {
    registrationInformation.remove(functionName.toUpperCase());
  }

  @Override
  public ServiceType getID() {
    return ServiceType.UDF_REGISTRATION_SERVICE;
  }

  private static UDFRegistrationService INSTANCE = null;

  public static synchronized UDFRegistrationService setupAndGetInstance(String ulogFileDir) {
    if (INSTANCE == null) {
      INSTANCE = new UDFRegistrationService(ulogFileDir);
    }
    return INSTANCE;
  }

  public static UDFRegistrationService getInstance() {
    return INSTANCE;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // SnapshotProcessor
  /////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws IOException {
    return SnapshotUtils.takeSnapshotForDir(
        ulogFileDir, snapshotDir.getAbsolutePath() + File.separator + "udf");
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws IOException {
    SnapshotUtils.loadSnapshotForDir(
        snapshotDir.getAbsolutePath() + File.separator + "udf", ulogFileDir);

    try {
      recovery();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
