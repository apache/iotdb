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

package org.apache.iotdb.commons.udf;

import org.apache.iotdb.commons.udf.builtin.BuiltinTimeSeriesGeneratingFunction;
import org.apache.iotdb.commons.udf.service.UDFClassLoader;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class UDFTable {
  private final Map<String, UDFInformation> udfInformationMap;

  /** maintain a map for creating instance */
  private final Map<String, Class<?>> functionToClassMap;

  public UDFTable() {
    udfInformationMap = new ConcurrentHashMap<>();
    functionToClassMap = new ConcurrentHashMap<>();
    registerBuiltinTimeSeriesGeneratingFunctions();
  }

  private void registerBuiltinTimeSeriesGeneratingFunctions() {
    for (BuiltinTimeSeriesGeneratingFunction builtinTimeSeriesGeneratingFunction :
        BuiltinTimeSeriesGeneratingFunction.values()) {
      String functionName = builtinTimeSeriesGeneratingFunction.getFunctionName();
      udfInformationMap.put(
          functionName,
          new UDFInformation(
              functionName.toUpperCase(),
              builtinTimeSeriesGeneratingFunction.getClassName(),
              true,
              false));
      functionToClassMap.put(
          functionName.toUpperCase(), builtinTimeSeriesGeneratingFunction.getFunctionClass());
    }
  }

  public void addUDFInformation(String functionName, UDFInformation udfInformation) {
    udfInformationMap.put(functionName.toUpperCase(), udfInformation);
  }

  public void removeUDFInformation(String functionName) {
    udfInformationMap.remove(functionName.toUpperCase());
  }

  public UDFInformation getUDFInformation(String functionName) {
    return udfInformationMap.get(functionName.toUpperCase());
  }

  public void addFunctionAndClass(String functionName, Class<?> clazz) {
    functionToClassMap.put(functionName.toUpperCase(), clazz);
  }

  public Class<?> getFunctionClass(String functionName) {
    return functionToClassMap.get(functionName.toUpperCase());
  }

  public void removeFunctionClass(String functionName) {
    functionToClassMap.remove(functionName.toUpperCase());
  }

  public void updateFunctionClass(UDFInformation udfInformation, UDFClassLoader classLoader)
      throws ClassNotFoundException {
    Class<?> functionClass = Class.forName(udfInformation.getClassName(), true, classLoader);
    functionToClassMap.put(udfInformation.getFunctionName().toUpperCase(), functionClass);
  }

  public UDFInformation[] getAllUDFInformation() {
    return udfInformationMap.values().toArray(new UDFInformation[0]);
  }

  public List<UDFInformation> getAllNonBuiltInUDFInformation() {
    return udfInformationMap.values().stream()
        .filter(udfInformation -> !udfInformation.isBuiltin())
        .collect(Collectors.toList());
  }

  public boolean containsUDF(String udfName) {
    return udfInformationMap.containsKey(udfName);
  }

  @TestOnly
  public Map<String, UDFInformation> getTable() {
    return udfInformationMap;
  }

  public void serializeUDFTable(OutputStream outputStream) throws IOException {
    List<UDFInformation> nonBuiltInUDFInformation = getAllNonBuiltInUDFInformation();
    ReadWriteIOUtils.write(nonBuiltInUDFInformation.size(), outputStream);
    for (UDFInformation udfInformation : nonBuiltInUDFInformation) {
      ReadWriteIOUtils.write(udfInformation.serialize(), outputStream);
    }
  }

  public void deserializeUDFTable(InputStream inputStream) throws IOException {
    int size = ReadWriteIOUtils.readInt(inputStream);
    while (size > 0) {
      UDFInformation udfInformation = UDFInformation.deserialize(inputStream);
      udfInformationMap.put(udfInformation.getFunctionName(), udfInformation);
      size--;
    }
  }

  // only clear external UDFs
  public void clear() {
    udfInformationMap.forEach(
        (K, V) -> {
          if (!V.isBuiltin()) {
            udfInformationMap.remove(K);
          }
        });
  }
}
