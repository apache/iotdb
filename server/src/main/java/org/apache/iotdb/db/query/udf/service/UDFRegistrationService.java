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

package org.apache.iotdb.db.query.udf.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.io.FileUtils;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.exception.UDFRegistrationException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.udf.api.UDF;
import org.apache.iotdb.db.query.udf.core.context.UDFContext;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UDFRegistrationService implements IService {

  private static final Logger logger = LoggerFactory.getLogger(UDFRegistrationService.class);

  private static final String ULOG_FILE_DIR =
      IoTDBDescriptor.getInstance().getConfig().getSystemDir()
          + File.separator + "udf" + File.separator;
  private static final String LOG_FILE_NAME = ULOG_FILE_DIR + "ulog.txt";
  private static final String TEMPORARY_LOG_FILE_NAME = LOG_FILE_NAME + ".tmp";

  private final ConcurrentHashMap<String, UDFRegistrationInformation> registrationInformation;

  private final ReentrantReadWriteLock lock;
  private UDFLogWriter temporaryLogWriter;

  private UDFClassLoader udfClassLoader;

  private UDFRegistrationService() {
    registrationInformation = new ConcurrentHashMap<>();
    lock = new ReentrantReadWriteLock();
  }

  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public void register(String functionName, String className, boolean isTemporary,
      boolean writeToTemporaryLogFile) throws UDFRegistrationException {
    UDFRegistrationInformation information = registrationInformation.get(functionName);
    if (information != null) {
      if (information.getClassName().equals(className)) {
        String errorMessage;
        if (information.isTemporary() == isTemporary) {
          errorMessage = String
              .format("UDF %s(%s) has already been registered successfully.",
                  functionName, className);
        } else {
          errorMessage = String.format(
              "Failed to register %sTEMPORARY UDF %s(%s), because a %sTEMPORARY UDF %s(%s) with the same function name and the class name has already been registered.",
              isTemporary ? "" : "non-", functionName, className,
              information.isTemporary() ? "" : "non-", information.getFunctionName(),
              information.getClassName());
        }
        logger.warn(errorMessage);
        throw new UDFRegistrationException(errorMessage);
      } else {
        String errorMessage = String.format(
            "Failed to register UDF %s(%s), because a UDF %s(%s) with the same function name but a different class name has already been registered.",
            functionName, className,
            information.getFunctionName(), information.getClassName());
        logger.warn(errorMessage);
        throw new UDFRegistrationException(errorMessage);
      }
    }

    Class<?> functionClass;
    try {
      udfClassLoader.refresh();
      functionClass = Class.forName(className, true, udfClassLoader);
      functionClass.getDeclaredConstructor().newInstance();
    } catch (IOException | InstantiationException | InvocationTargetException | NoSuchMethodException | IllegalAccessException | ClassNotFoundException e) {
      String errorMessage = String.format(
          "Failed to register UDF %s(%s), because its instance can not be constructed successfully. Exception: %s",
          functionName, className, e.toString());
      logger.warn(errorMessage);
      throw new UDFRegistrationException(errorMessage);
    }

    registrationInformation.put(functionName,
        new UDFRegistrationInformation(functionName, className, functionClass, isTemporary));

    if (writeToTemporaryLogFile && !isTemporary) {
      try {
        appendRegistrationLog(functionName, className);
      } catch (IOException e) {
        registrationInformation.remove(functionName);
        String errorMessage = String
            .format("Failed to append UDF log when registering UDF %s(%s), because %s",
                functionName, className, e.toString());
        logger.error(errorMessage);
        throw new UDFRegistrationException(errorMessage, e);
      }
    }
  }

  public void deregister(String functionName) throws UDFRegistrationException {
    UDFRegistrationInformation information = registrationInformation.remove(functionName);
    if (information == null) {
      String errorMessage = String.format("UDF %s does not exist.", functionName);
      logger.warn(errorMessage);
      throw new UDFRegistrationException(errorMessage);
    }

    if (!information.isTemporary()) {
      try {
        appendDeregistrationLog(functionName);
      } catch (IOException e) {
        registrationInformation.put(functionName, information);
        String errorMessage = String
            .format("Failed to append UDF log when deregistering UDF %s, because %s",
                functionName, e.toString());
        logger.error(errorMessage);
        throw new UDFRegistrationException(errorMessage, e);
      }
    }
  }

  private void appendRegistrationLog(String functionName, String className) throws IOException {
    lock.writeLock().lock();
    try {
      temporaryLogWriter.register(functionName, className);
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void appendDeregistrationLog(String functionName) throws IOException {
    lock.writeLock().lock();
    try {
      temporaryLogWriter.deregister(functionName);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public UDF reflect(UDFContext context) throws QueryProcessException {
    UDFRegistrationInformation information = registrationInformation.get(context.getName());
    if (information == null) {
      String errorMessage = String
          .format("Failed to reflect UDF instance, because UDF %s has not been registered.",
              context.getName());
      logger.warn(errorMessage);
      throw new QueryProcessException(errorMessage);
    }

    Thread.currentThread().setContextClassLoader(udfClassLoader);
    try {
      return (UDF) information.getFunctionClass().getDeclaredConstructor().newInstance();
    } catch (InstantiationException | InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
      String errorMessage = String.format("Failed to reflect UDF %s(%s) instance, because %s",
          context.getName(), information.getClassName(), e.toString());
      logger.warn(errorMessage);
      throw new QueryProcessException(errorMessage);
    }
  }

  public UDFRegistrationInformation[] getRegistrationInformation() {
    return registrationInformation.values().toArray(new UDFRegistrationInformation[0]);
  }

  @Override
  public void start() throws StartupException {
    try {
      udfClassLoader = new UDFClassLoader(parseLibRoot());
      makeDirIfNecessary();
      doRecovery();
      temporaryLogWriter = new UDFLogWriter(TEMPORARY_LOG_FILE_NAME);
    } catch (Exception e) {
      throw new StartupException(e);
    }
  }

  private String parseLibRoot() {
    String jarPath = (new File(
        getClass().getProtectionDomain().getCodeSource().getLocation().getPath()))
        .getAbsolutePath();
    int lastIndex = jarPath.lastIndexOf(File.separatorChar);
    String libPath = jarPath.substring(0, lastIndex + 1);
    logger.info("System lib root: {}", libPath);
    return libPath;
  }

  private void makeDirIfNecessary() throws IOException {
    File file = SystemFileFactory.INSTANCE.getFile(ULOG_FILE_DIR);
    if (file.exists() && file.isDirectory()) {
      return;
    }
    FileUtils.forceMkdir(file);
  }

  private void doRecovery() throws IOException {
    File logFile = SystemFileFactory.INSTANCE.getFile(LOG_FILE_NAME);
    File temporaryLogFile = SystemFileFactory.INSTANCE.getFile(TEMPORARY_LOG_FILE_NAME);

    if (temporaryLogFile.exists()) {
      if (logFile.exists()) {
        FileUtils.deleteQuietly(logFile);
      }
      recoveryFromLogFile(temporaryLogFile);
    } else if (logFile.exists()) {
      recoveryFromLogFile(logFile);
      FSFactoryProducer.getFSFactory().moveFile(logFile, temporaryLogFile);
    }
  }

  private void recoveryFromLogFile(File logFile) throws IOException {
    HashMap<String, String> recoveredUDFs = new HashMap<>();

    try (BufferedReader reader = new BufferedReader(new FileReader(logFile))) {
      String line;
      while ((line = reader.readLine()) != null) {
        String[] data = line.split(",");
        byte type = Byte.parseByte(data[0]);
        if (type == UDFLogWriter.REGISTER_TYPE) {
          recoveredUDFs.put(data[1], data[2]);
        } else if (type == UDFLogWriter.DEREGISTER_TYPE) {
          recoveredUDFs.remove(data[1]);
        }
      }
    }

    for (Entry<String, String> udf : recoveredUDFs.entrySet()) {
      try {
        register(udf.getKey(), udf.getValue(), false, false);
      } catch (UDFRegistrationException ignored) {
        // ignored
      }
    }
  }

  @Override
  public void stop() {
    try {
      writeLogFile();
      temporaryLogWriter.close();
      temporaryLogWriter.deleteLogFile();
    } catch (IOException ignored) {
      // ignored
    }
  }

  private void writeLogFile() throws IOException {
    UDFLogWriter logWriter = new UDFLogWriter(LOG_FILE_NAME);
    for (UDFRegistrationInformation information : registrationInformation.values()) {
      if (information.isTemporary()) {
        continue;
      }
      logWriter.register(information.getFunctionName(), information.getClassName());
    }
    logWriter.close();
  }

  @TestOnly
  public void deregisterAll() throws UDFRegistrationException {
    for (UDFRegistrationInformation information : getRegistrationInformation()) {
      deregister(information.getFunctionName());
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.UDF_REGISTRATION_SERVICE;
  }

  public static UDFRegistrationService getInstance() {
    return UDFRegistrationService.UDFRegistrationServiceHelper.INSTANCE;
  }

  private static class UDFRegistrationServiceHelper {

    private static final UDFRegistrationService INSTANCE = new UDFRegistrationService();

    private UDFRegistrationServiceHelper() {
    }
  }
}
