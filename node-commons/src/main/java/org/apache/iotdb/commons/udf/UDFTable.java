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

import org.apache.iotdb.commons.udf.service.UDFClassLoader;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class UDFTable {
  private final Map<String, UDFInformation> udfInformationMap;

  /** maintain a map for creating instance */
  private final Map<String, Class<?>> functionToClassMap;

  public UDFTable() {
    udfInformationMap = new ConcurrentHashMap<>();
    functionToClassMap = new ConcurrentHashMap<>();
  }

  public void addUDFInformation(String functionName, UDFInformation udfInformation) {
    udfInformationMap.put(functionName.toUpperCase(), udfInformation);
  }

  public UDFInformation removeUDFInformation(String functionName) {
    return udfInformationMap.remove(functionName.toUpperCase());
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
    functionToClassMap.put(udfInformation.getClassName(), functionClass);
  }

  public UDFInformation[] getAllUDFInformation() {
    return udfInformationMap.values().toArray(new UDFInformation[0]);
  }
}
