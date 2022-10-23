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

import org.apache.iotdb.commons.executable.ExecutableResource;
import org.apache.iotdb.commons.udf.UDFInformation;
import org.apache.iotdb.commons.udf.UDFTable;
import org.apache.iotdb.commons.udf.builtin.BuiltinAggregationFunction;
import org.apache.iotdb.commons.udf.builtin.BuiltinTimeSeriesGeneratingFunction;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.udf.api.UDF;
import org.apache.iotdb.udf.api.exception.UDFManagementException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class UDFManagementService {

  private static final Logger LOGGER = LoggerFactory.getLogger(UDFManagementService.class);

  private final ReentrantLock lock;
  private final UDFTable udfTable;

  private UDFManagementService() {
    lock = new ReentrantLock();
    udfTable = new UDFTable();
    registerBuiltinTimeSeriesGeneratingFunctions();
  }

  public void acquireLock() {
    lock.lock();
  }

  public void releaseLock() {
    lock.unlock();
  }

  /** invoked by config leader for validation before registration */
  public void validate(UDFInformation udfInformation) {
    validateFunctionName(udfInformation);
    checkIfRegistered(udfInformation);
  }

  public void register(UDFInformation udfInformation) throws UDFManagementException {
    validateFunctionName(udfInformation);
    checkIfRegistered(udfInformation);
    doRegister(udfInformation);
  }

  private static void validateFunctionName(UDFInformation udfInformation)
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
    String functionName = udfInformation.getFunctionName();
    String className = udfInformation.getClassName();
    UDFInformation information = udfTable.getUDFInformation(functionName);
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
    throw new UDFManagementException(errorMessage);
  }

  private void downloadExecutableResources(
      String functionName,
      String className,
      List<String> uris,
      UDFExecutableManager udfExecutableManager)
      throws UDFManagementException {
    if (uris.isEmpty()) {
      return;
    }

    try {
      final ExecutableResource resource = udfExecutableManager.request(uris);
      try {
        udfExecutableManager.removeUDFJarFromExtLibDir(functionName);
        udfExecutableManager.moveTempDirToExtLibDir(resource, functionName);
      } catch (Exception innerException) {
        udfExecutableManager.removeUDFJarFromExtLibDir(functionName);
        udfExecutableManager.removeFromTemporaryLibRoot(resource);
        throw innerException;
      }
    } catch (Exception outerException) {
      String errorMessage =
          String.format(
              "Failed to register UDF %s(%s) because failed to fetch UDF executables(%s)",
              functionName, className, uris);
      LOGGER.warn(errorMessage, outerException);
      throw new UDFManagementException(errorMessage, outerException);
    }
  }

  private void doRegister(UDFInformation udfInformation) throws UDFManagementException {
    String functionName = udfInformation.getFunctionName();
    String className = udfInformation.getClassName();
    try {
      acquireLock();
      UDFClassLoader currentActiveClassLoader =
          UDFClassLoaderManager.getInstance().updateAndGetActiveClassLoader();
      updateAllRegisteredClasses(currentActiveClassLoader);

      Class<?> functionClass = Class.forName(className, true, currentActiveClassLoader);
      functionClass.getDeclaredConstructor().newInstance();
      udfTable.addUDFInformation(
          functionName, new UDFInformation(functionName, className, false, functionClass));
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
      throw new UDFManagementException(errorMessage);
    } finally {
      releaseLock();
    }
  }

  private void updateAllRegisteredClasses(UDFClassLoader activeClassLoader)
      throws ClassNotFoundException {
    for (UDFInformation information : getAllUDFInformation()) {
      if (!information.isBuiltin()) {
        information.updateFunctionClass(activeClassLoader);
      }
    }
  }

  public void deregister(String functionName) throws UDFManagementException {
    try {
      UDFInformation information = udfTable.getUDFInformation(functionName);
      if (information != null && information.isBuiltin()) {
        String errorMessage =
            String.format("Built-in function %s can not be deregistered.", functionName);
        LOGGER.warn(errorMessage);
        throw new UDFManagementException(errorMessage);
      }

      udfTable.removeUDFInformation(functionName);
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

  public UDFInformation[] getAllUDFInformation() {
    return udfTable.getAllUDFInformation();
  }

  private void registerBuiltinTimeSeriesGeneratingFunctions() {
    for (BuiltinTimeSeriesGeneratingFunction builtinTimeSeriesGeneratingFunction :
        BuiltinTimeSeriesGeneratingFunction.values()) {
      String functionName = builtinTimeSeriesGeneratingFunction.getFunctionName();
      udfTable.addUDFInformation(
          functionName,
          new UDFInformation(
              functionName,
              builtinTimeSeriesGeneratingFunction.getClassName(),
              true,
              builtinTimeSeriesGeneratingFunction.getFunctionClass()));
    }
  }

  @TestOnly
  public void deregisterAll() throws UDFManagementException {
    for (UDFInformation information : getAllUDFInformation()) {
      if (!information.isBuiltin()) {
        deregister(information.getFunctionName());
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
