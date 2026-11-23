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

import org.apache.iotdb.common.rpc.thrift.Model;
import org.apache.iotdb.commons.utils.TestOnly;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * UDFTable is a table that stores UDF information. On DataNode, it stores all UDF information. On
 * ConfigNode, it does not store built-in UDF information.
 */
public class UDFTable {

  /** model -> functionName -> information * */
  private final Map<Model, Map<String, UDFInformation>> udfInformationMap;

  /** maintain a map for creating instance, model -> functionName -> class */
  private volatile Map<Model, Map<String, Class<?>>> functionToClassMap;

  public UDFTable() {
    udfInformationMap = new ConcurrentHashMap<>();
    functionToClassMap = emptyFunctionClassMap();
    udfInformationMap.put(Model.TREE, new ConcurrentHashMap<>());
    udfInformationMap.put(Model.TABLE, new ConcurrentHashMap<>());
  }

  private static Map<Model, Map<String, Class<?>>> emptyFunctionClassMap() {
    Map<Model, Map<String, Class<?>>> functionClassMap = new HashMap<>();
    functionClassMap.put(Model.TREE, Collections.emptyMap());
    functionClassMap.put(Model.TABLE, Collections.emptyMap());
    return Collections.unmodifiableMap(functionClassMap);
  }

  public synchronized void addUDFInformation(String functionName, UDFInformation udfInformation) {
    if (udfInformation.getUdfType().isTreeModel()) {
      udfInformationMap.get(Model.TREE).put(functionName.toUpperCase(), udfInformation);
    } else {
      udfInformationMap.get(Model.TABLE).put(functionName.toUpperCase(), udfInformation);
    }
  }

  public synchronized void removeUDFInformation(Model model, String functionName) {
    udfInformationMap.get(model).remove(functionName.toUpperCase());
  }

  public synchronized UDFInformation getUDFInformation(Model model, String functionName) {
    return udfInformationMap.get(model).get(functionName.toUpperCase());
  }

  public synchronized void addFunctionAndClass(Model model, String functionName, Class<?> clazz) {
    Map<Model, Map<String, Class<?>>> newFunctionClassMap = new HashMap<>(functionToClassMap);
    Map<String, Class<?>> modelFunctionClassMap = new HashMap<>(functionToClassMap.get(model));
    modelFunctionClassMap.put(functionName.toUpperCase(), clazz);
    newFunctionClassMap.put(model, Collections.unmodifiableMap(modelFunctionClassMap));
    functionToClassMap = Collections.unmodifiableMap(newFunctionClassMap);
  }

  public synchronized Class<?> getFunctionClass(Model model, String functionName) {
    return functionToClassMap.get(model).get(functionName.toUpperCase());
  }

  public synchronized void removeFunctionClass(Model model, String functionName) {
    Map<Model, Map<String, Class<?>>> newFunctionClassMap = new HashMap<>(functionToClassMap);
    Map<String, Class<?>> modelFunctionClassMap = new HashMap<>(functionToClassMap.get(model));
    modelFunctionClassMap.remove(functionName.toUpperCase());
    newFunctionClassMap.put(model, Collections.unmodifiableMap(modelFunctionClassMap));
    functionToClassMap = Collections.unmodifiableMap(newFunctionClassMap);
  }

  private static Map<Model, Map<String, Class<?>>> createFunctionClassSnapshot(
      Map<Model, Map<String, Class<?>>> newFunctionClassMap) {
    Map<Model, Map<String, Class<?>>> snapshot = new HashMap<>();
    for (Model model : Model.values()) {
      snapshot.put(
          model, Collections.unmodifiableMap(new HashMap<>(newFunctionClassMap.get(model))));
    }
    return Collections.unmodifiableMap(snapshot);
  }

  public synchronized void publishUDF(
      String functionName,
      UDFInformation udfInformation,
      Map<Model, Map<String, Class<?>>> newFunctionClassMap) {
    Map<Model, Map<String, Class<?>>> functionClassSnapshot =
        createFunctionClassSnapshot(newFunctionClassMap);
    addUDFInformation(functionName, udfInformation);
    functionToClassMap = functionClassSnapshot;
  }

  public synchronized List<UDFInformation> getUDFInformationList(Model model) {
    return new ArrayList<>(udfInformationMap.get(model).values());
  }

  public synchronized List<UDFInformation> getAllInformationList() {
    return udfInformationMap.values().stream()
        .flatMap(map -> map.values().stream())
        .collect(Collectors.toList());
  }

  public synchronized boolean containsUDF(Model model, String udfName) {
    return udfInformationMap.get(model).containsKey(udfName.toUpperCase());
  }

  @TestOnly
  public Map<Model, Map<String, UDFInformation>> getTable() {
    return udfInformationMap;
  }

  public void serializeUDFTable(OutputStream outputStream) throws IOException {
    List<UDFInformation> nonBuiltInUDFInformation = getAllInformationList();
    ReadWriteIOUtils.write(nonBuiltInUDFInformation.size(), outputStream);
    for (UDFInformation udfInformation : nonBuiltInUDFInformation) {
      ReadWriteIOUtils.write(udfInformation.serialize(), outputStream);
    }
  }

  public synchronized void deserializeUDFTable(InputStream inputStream) throws IOException {
    int size = ReadWriteIOUtils.readInt(inputStream);
    while (size > 0) {
      UDFInformation udfInformation = UDFInformation.deserialize(inputStream);
      if (udfInformation.getUdfType().isTreeModel()) {
        udfInformationMap.get(Model.TREE).put(udfInformation.getFunctionName(), udfInformation);
      } else {
        udfInformationMap.get(Model.TABLE).put(udfInformation.getFunctionName(), udfInformation);
      }
      size--;
    }
  }

  public synchronized void clear() {
    udfInformationMap.get(Model.TREE).clear();
    udfInformationMap.get(Model.TABLE).clear();
    functionToClassMap = emptyFunctionClassMap();
  }
}
