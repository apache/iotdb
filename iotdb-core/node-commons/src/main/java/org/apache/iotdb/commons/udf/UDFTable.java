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
import org.apache.iotdb.commons.udf.service.UDFClassLoader;
import org.apache.iotdb.commons.utils.TestOnly;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
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
  private final Map<Model, Map<String, Class<?>>> functionToClassMap;

  public UDFTable() {
    udfInformationMap = new ConcurrentHashMap<>();
    functionToClassMap = new ConcurrentHashMap<>();
    udfInformationMap.put(Model.TREE, new ConcurrentHashMap<>());
    udfInformationMap.put(Model.TABLE, new ConcurrentHashMap<>());
    functionToClassMap.put(Model.TREE, new ConcurrentHashMap<>());
    functionToClassMap.put(Model.TABLE, new ConcurrentHashMap<>());
  }

  public void addUDFInformation(String functionName, UDFInformation udfInformation) {
    if (udfInformation.getUdfType().isTreeModel()) {
      udfInformationMap.get(Model.TREE).put(functionName.toUpperCase(), udfInformation);
    } else {
      udfInformationMap.get(Model.TABLE).put(functionName.toUpperCase(), udfInformation);
    }
  }

  public void removeUDFInformation(Model model, String functionName) {
    udfInformationMap.get(model).remove(functionName.toUpperCase());
  }

  public UDFInformation getUDFInformation(Model model, String functionName) {
    return udfInformationMap.get(model).get(functionName.toUpperCase());
  }

  public void addFunctionAndClass(Model model, String functionName, Class<?> clazz) {
    functionToClassMap.get(model).put(functionName.toUpperCase(), clazz);
  }

  public Class<?> getFunctionClass(Model model, String functionName) {
    return functionToClassMap.get(model).get(functionName.toUpperCase());
  }

  public void removeFunctionClass(Model model, String functionName) {
    functionToClassMap.get(model).remove(functionName.toUpperCase());
  }

  public void updateFunctionClass(UDFInformation udfInformation, UDFClassLoader classLoader)
      throws ClassNotFoundException {
    Class<?> functionClass = Class.forName(udfInformation.getClassName(), true, classLoader);
    if (udfInformation.getUdfType().isTreeModel()) {
      functionToClassMap
          .get(Model.TREE)
          .put(udfInformation.getFunctionName().toUpperCase(), functionClass);
    } else {
      functionToClassMap
          .get(Model.TABLE)
          .put(udfInformation.getFunctionName().toUpperCase(), functionClass);
    }
  }

  public List<UDFInformation> getUDFInformationList(Model model) {
    return new ArrayList<>(udfInformationMap.get(model).values());
  }

  public List<UDFInformation> getAllInformationList() {
    return udfInformationMap.values().stream()
        .flatMap(map -> map.values().stream())
        .collect(Collectors.toList());
  }

  public boolean containsUDF(Model model, String udfName) {
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

  public void deserializeUDFTable(InputStream inputStream) throws IOException {
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

  public void clear() {
    udfInformationMap.get(Model.TREE).clear();
    udfInformationMap.get(Model.TABLE).clear();
    functionToClassMap.get(Model.TREE).clear();
    functionToClassMap.get(Model.TABLE).clear();
  }
}
