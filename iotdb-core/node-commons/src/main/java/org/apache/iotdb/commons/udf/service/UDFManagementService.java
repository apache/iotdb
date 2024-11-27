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

import org.apache.iotdb.common.rpc.thrift.Model;
import org.apache.iotdb.commons.udf.UDFInformation;
import org.apache.iotdb.commons.udf.UDFTable;
import org.apache.iotdb.commons.udf.UDFType;
import org.apache.iotdb.commons.udf.builtin.BuiltinAggregationFunction;
import org.apache.iotdb.commons.udf.builtin.BuiltinScalarFunction;
import org.apache.iotdb.commons.udf.builtin.BuiltinTimeSeriesGeneratingFunction;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.udf.api.UDF;
import org.apache.iotdb.udf.api.exception.UDFManagementException;
import org.apache.iotdb.udf.api.relational.SQLFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantLock;

public class UDFManagementService {

  private static final Logger LOGGER = LoggerFactory.getLogger(UDFManagementService.class);

  private final ReentrantLock lock;
  private final UDFTable udfTable;

  private UDFManagementService() {
    lock = new ReentrantLock();
    udfTable = new UDFTable();
    // register tree model built-in functions
    for (BuiltinTimeSeriesGeneratingFunction builtinTimeSeriesGeneratingFunction :
        BuiltinTimeSeriesGeneratingFunction.values()) {
      String functionName = builtinTimeSeriesGeneratingFunction.getFunctionName();
      udfTable.addUDFInformation(
          functionName,
          new UDFInformation(
              functionName.toUpperCase(),
              builtinTimeSeriesGeneratingFunction.getClassName(),
              UDFType.TREE_BUILT_IN));
      udfTable.addFunctionAndClass(
          Model.TREE, functionName, builtinTimeSeriesGeneratingFunction.getFunctionClass());
    }
  }

  public void acquireLock() {
    lock.lock();
  }

  public void releaseLock() {
    lock.unlock();
  }

  public void register(UDFInformation udfInformation, ByteBuffer jarFile) throws Exception {
    Model model = checkAndGetModel(udfInformation);
    try {
      acquireLock();
      checkIfRegistered(model, udfInformation);
      saveJarFile(udfInformation.getJarName(), jarFile);
      doRegister(model, udfInformation);
    } finally {
      releaseLock();
    }
  }

  public Model checkAndGetModel(UDFInformation udfInformation) {
    if (!udfInformation.isAvailable()) {
      throw new UDFManagementException("UDFInformation is not available");
    }
    Model model;
    if (udfInformation.getUdfType().isTreeModel()) {
      model = Model.TREE;
    } else {
      model = Model.TABLE;
    }
    return model;
  }

  private Class<?> getBaseClass(Model model) {
    if (Model.TREE.equals(model)) {
      return UDF.class;
    } else {
      return SQLFunction.class;
    }
  }

  public boolean checkIsBuiltInFunctionName(Model model, String functionName)
      throws UDFManagementException {
    if (Model.TREE.equals(model)) {
      return BuiltinAggregationFunction.getNativeFunctionNames()
              .contains(functionName.toLowerCase())
          || BuiltinTimeSeriesGeneratingFunction.getNativeFunctionNames()
              .contains(functionName.toUpperCase())
          || BuiltinScalarFunction.getNativeFunctionNames().contains(functionName.toLowerCase());
    } else {
      // TODO: Table model UDF
      return false;
    }
  }

  private void checkIfRegistered(Model model, UDFInformation udfInformation)
      throws UDFManagementException {
    if (checkIsBuiltInFunctionName(model, udfInformation.getFunctionName())) {
      String errorMessage =
          String.format(
              "Failed to register UDF %s(%s), because the given function name conflicts with the built-in function name",
              udfInformation.getFunctionName(), udfInformation.getClassName());

      LOGGER.warn(errorMessage);
      throw new UDFManagementException(errorMessage);
    }
    String functionName = udfInformation.getFunctionName();
    String className = udfInformation.getClassName();
    UDFInformation information = udfTable.getUDFInformation(model, functionName);
    if (information == null) {
      return;
    }

    if (UDFExecutableManager.getInstance().hasFileUnderInstallDir(udfInformation.getJarName())
        && UDFExecutableManager.getInstance().isLocalJarConflicted(udfInformation)) {
      String errorMessage =
          String.format(
              "Failed to register function %s(%s), "
                  + "because existed md5 of jar file for function %s is different from the new jar file. ",
              functionName, className, functionName);
      LOGGER.warn(errorMessage);
      throw new UDFManagementException(errorMessage);
    }
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
  public void doRegister(Model model, UDFInformation udfInformation) throws UDFManagementException {
    String functionName = udfInformation.getFunctionName();
    String className = udfInformation.getClassName();
    try {
      UDFClassLoader currentActiveClassLoader =
          UDFClassLoaderManager.getInstance().updateAndGetActiveClassLoader();
      updateAllRegisteredClasses(model, currentActiveClassLoader);

      Class<?> functionClass = Class.forName(className, true, currentActiveClassLoader);

      // ensure that it is a UDF class
      getBaseClass(model).cast(functionClass.getDeclaredConstructor().newInstance());
      udfTable.addUDFInformation(functionName, udfInformation);
      udfTable.addFunctionAndClass(model, functionName, functionClass);
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

  private void updateAllRegisteredClasses(Model model, UDFClassLoader activeClassLoader)
      throws ClassNotFoundException {
    for (UDFInformation information : getUDFInformation(model)) {
      udfTable.updateFunctionClass(information, activeClassLoader);
    }
  }

  public void deregister(Model model, String functionName, boolean needToDeleteJar)
      throws Exception {
    try {
      acquireLock();
      UDFInformation information = udfTable.getUDFInformation(Model.TREE, functionName);
      if (information == null) {
        return;
      }
      udfTable.removeUDFInformation(model, functionName);
      udfTable.removeFunctionClass(model, functionName);
      if (needToDeleteJar) {
        UDFExecutableManager.getInstance().removeFileUnderLibRoot(information.getJarName());
        UDFExecutableManager.getInstance()
            .removeFileUnderTemporaryRoot(functionName.toUpperCase() + ".txt");
      }
    } finally {
      releaseLock();
    }
  }

  public <T> T reflect(String functionName, Class<T> clazz) {
    Model model;
    if (UDF.class.isAssignableFrom(clazz)) {
      model = Model.TREE;
    } else if (SQLFunction.class.isAssignableFrom(clazz)) {
      model = Model.TABLE;
    } else {
      throw new UDFManagementException(
          "Unsupported UDF class type. Only UDF and SQLFunction are supported.");
    }
    UDFInformation information = udfTable.getUDFInformation(model, functionName);
    if (information == null) {
      String errorMessage =
          String.format(
              "Failed to reflect UDF instance, because UDF %s has not been registered.",
              functionName.toUpperCase());
      LOGGER.warn(errorMessage);
      throw new RuntimeException(errorMessage);
    }

    try {
      return clazz.cast(
          udfTable
              .getFunctionClass(Model.TREE, functionName)
              .getDeclaredConstructor()
              .newInstance());
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

  public UDFInformation[] getUDFInformation(Model model) {
    return udfTable.getUDFInformationList(model).toArray(new UDFInformation[0]);
  }

  @TestOnly
  public void deregisterAll() throws UDFManagementException {
    for (UDFInformation information : getUDFInformation(Model.TREE)) {
      try {
        deregister(Model.TREE, information.getFunctionName(), false);
      } catch (Exception e) {
        throw new UDFManagementException(e.getMessage());
      }
    }
    for (UDFInformation information : getUDFInformation(Model.TABLE)) {
      try {
        deregister(Model.TABLE, information.getFunctionName(), false);
      } catch (Exception e) {
        throw new UDFManagementException(e.getMessage());
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
