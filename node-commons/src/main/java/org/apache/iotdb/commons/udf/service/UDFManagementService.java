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

import org.apache.iotdb.commons.udf.UDFInformation;
import org.apache.iotdb.commons.udf.UDFTable;
import org.apache.iotdb.commons.udf.builtin.BuiltinAggregationFunction;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.udf.api.UDF;
import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.exception.UDFManagementException;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class UDFManagementService {

  private static final Logger LOGGER = LoggerFactory.getLogger(UDFManagementService.class);

  private final ReentrantLock lock;
  private final UDFTable udfTable;

  private UDFManagementService() {
    lock = new ReentrantLock();
    udfTable = new UDFTable();
  }

  public void acquireLock() {
    lock.lock();
  }

  public void releaseLock() {
    lock.unlock();
  }

  /** invoked by config leader for validation before registration */
  public void validate(UDFInformation udfInformation) {
    try {
      acquireLock();
      checkIfRegistered(udfInformation);
    } finally {
      releaseLock();
    }
  }

  public void register(UDFInformation udfInformation, ByteBuffer jarFile) throws Exception {
    try {
      acquireLock();
      checkIfRegistered(udfInformation);
      saveJarFile(udfInformation.getJarName(), jarFile);
      doRegister(udfInformation);
    } finally {
      releaseLock();
    }
  }

  /** temp code for stand-alone */
  public void register(UDFInformation udfInformation) throws Exception {
    try {
      acquireLock();
      checkIfRegistered(udfInformation);
      doRegister(udfInformation);
    } finally {
      releaseLock();
    }
  }

  private void checkIsBuiltInAggregationFunctionName(UDFInformation udfInformation)
      throws UDFManagementException {
    String functionName = udfInformation.getFunctionName();
    String className = udfInformation.getClassName();
    if (!BuiltinAggregationFunction.getNativeFunctionNames().contains(functionName.toLowerCase())) {
      return;
    }

    String errorMessage =
        String.format(
            "Failed to register UDF %s(%s), because the given function name conflicts with the built-in function name",
            functionName, className);

    LOGGER.warn(errorMessage);
    throw new UDFManagementException(errorMessage);
  }

  private void checkIfRegistered(UDFInformation udfInformation) throws UDFManagementException {
    checkIsBuiltInAggregationFunctionName(udfInformation);
    String functionName = udfInformation.getFunctionName();
    String className = udfInformation.getClassName();
    UDFInformation information = udfTable.getUDFInformation(functionName);
    if (information == null) {
      return;
    }

    if (information.isBuiltin()) {
      String errorMessage =
          String.format(
              "Failed to register UDF %s(%s), because the given function name is the same as a built-in UDF function name.",
              functionName, className);
      LOGGER.warn(errorMessage);
      throw new UDFManagementException(errorMessage);
    } else {
      if (UDFExecutableManager.getInstance().hasFileUnderInstallDir(udfInformation.getJarName())
          && isLocalJarConflicted(udfInformation)) {
        String errorMessage =
            String.format(
                "Failed to register function %s, "
                    + "because existed md5 of jar file for function %s is different from the new jar file. ",
                functionName, functionName);
        LOGGER.warn(errorMessage);
        throw new UDFManagementException(errorMessage);
      }
    }
  }

  /** check whether local jar is correct according to md5 */
  public boolean isLocalJarConflicted(UDFInformation udfInformation) throws UDFManagementException {
    String functionName = udfInformation.getFunctionName();
    // A jar with the same name exists, we need to check md5
    String existedMd5 = "";
    String md5FilePath = functionName + ".txt";

    // if meet error when reading md5 from txt, we need to compute it again
    boolean hasComputed = false;
    if (UDFExecutableManager.getInstance().hasFileUnderTemporaryRoot(md5FilePath)) {
      try {
        existedMd5 =
            UDFExecutableManager.getInstance().readTextFromFileUnderTemporaryRoot(md5FilePath);
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
                        UDFExecutableManager.getInstance().getInstallDir()
                            + File.separator
                            + udfInformation.getJarName())));
        // save the md5 in a txt under trigger temporary lib
        UDFExecutableManager.getInstance()
            .saveTextAsFileUnderTemporaryRoot(existedMd5, md5FilePath);
      } catch (IOException e) {
        String errorMessage =
            String.format(
                "Failed to registered function %s, "
                    + "because error occurred when trying to compute md5 of jar file for function %s ",
                functionName, functionName);
        LOGGER.warn(errorMessage, e);
        throw new UDFManagementException(errorMessage);
      }
    }
    return !existedMd5.equals(udfInformation.getJarMD5());
  }

  private void saveJarFile(String jarName, ByteBuffer byteBuffer) throws IOException {
    if (byteBuffer != null) {
      UDFExecutableManager.getInstance().saveToInstallDir(byteBuffer, jarName);
    }
  }

  /**
   * Only call this method directly for registering new data node, otherwise you need to call
   * register().
   */
  public void doRegister(UDFInformation udfInformation) throws UDFManagementException {
    String functionName = udfInformation.getFunctionName();
    String className = udfInformation.getClassName();
    try {
      UDFClassLoader currentActiveClassLoader =
          UDFClassLoaderManager.getInstance().updateAndGetActiveClassLoader();
      updateAllRegisteredClasses(currentActiveClassLoader);

      Class<?> functionClass = Class.forName(className, true, currentActiveClassLoader);

      // ensure that it is a UDF class
      UDTF udtf = (UDTF) functionClass.getDeclaredConstructor().newInstance();
      udfTable.addUDFInformation(functionName, udfInformation);
      udfTable.addFunctionAndClass(functionName, functionClass);
    } catch (IOException
        | InstantiationException
        | InvocationTargetException
        | NoSuchMethodException
        | IllegalAccessException
        | ClassNotFoundException
        | ClassCastException e) {
      String errorMessage =
          String.format(
              "Failed to register UDF %s(%s), because its instance can not be constructed successfully. Exception: %s",
              functionName.toUpperCase(), className, e);
      LOGGER.warn(errorMessage, e);
      throw new UDFManagementException(errorMessage);
    }
  }

  private void updateAllRegisteredClasses(UDFClassLoader activeClassLoader)
      throws ClassNotFoundException {
    for (UDFInformation information : getAllUDFInformation()) {
      if (!information.isBuiltin()) {
        udfTable.updateFunctionClass(information, activeClassLoader);
      }
    }
  }

  public void deregister(String functionName, boolean needToDeleteJar) throws Exception {
    try {
      acquireLock();
      UDFInformation information = udfTable.getUDFInformation(functionName);
      if (information == null) {
        return;
      }
      if (information.isBuiltin()) {
        String errorMessage =
            String.format(
                "Built-in function %s can not be deregistered.", functionName.toUpperCase());
        LOGGER.warn(errorMessage);
        throw new UDFManagementException(errorMessage);
      }
      udfTable.removeUDFInformation(functionName);
      udfTable.removeFunctionClass(functionName);
      if (needToDeleteJar) {
        UDFExecutableManager.getInstance().removeFileUnderLibRoot(information.getJarName());
        UDFExecutableManager.getInstance()
            .removeFileUnderTemporaryRoot(functionName.toUpperCase() + ".txt");
      }
    } finally {
      releaseLock();
    }
  }

  public UDF reflect(String functionName) {
    UDFInformation information = udfTable.getUDFInformation(functionName);
    if (information == null) {
      String errorMessage =
          String.format(
              "Failed to reflect UDF instance, because UDF %s has not been registered.",
              functionName.toUpperCase());
      LOGGER.warn(errorMessage);
      throw new RuntimeException(errorMessage);
    }

    try {
      return (UDF) udfTable.getFunctionClass(functionName).getDeclaredConstructor().newInstance();
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

  public UDFInformation[] getAllUDFInformation() {
    return udfTable.getAllUDFInformation();
  }

  public List<UDFInformation> getAllBuiltInTimeSeriesGeneratingInformation() {
    return Arrays.stream(getAllUDFInformation())
        .filter(UDFInformation::isBuiltin)
        .collect(Collectors.toList());
  }

  public boolean isUDTF(String functionName)
      throws NoSuchMethodException, InvocationTargetException, InstantiationException,
          IllegalAccessException {
    return udfTable.getFunctionClass(functionName).getDeclaredConstructor().newInstance()
        instanceof UDTF;
  }

  /** always returns false for now */
  public boolean isUDAF(String functionName) {
    return false;
  }

  @TestOnly
  public void deregisterAll() throws UDFManagementException {
    for (UDFInformation information : getAllUDFInformation()) {
      if (!information.isBuiltin()) {
        try {
          deregister(information.getFunctionName(), false);
        } catch (Exception e) {
          throw new UDFManagementException(e.getMessage());
        }
      }
    }
  }

  private static class UDFManagementServiceHolder {
    private static final UDFManagementService INSTANCE = new UDFManagementService();
  }

  public static UDFManagementService getInstance() {
    return UDFManagementServiceHolder.INSTANCE;
  }
}
