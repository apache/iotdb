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

package org.apache.iotdb.commons.pipe.plugin;

import org.apache.iotdb.commons.pipe.plugin.service.PipePluginClassLoader;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PipePluginTable {

  private final Map<String, PipePluginInformation> pipePluginInformationMap;

  private final Map<String, Class<?>> pluginToClassMap;

  public PipePluginTable() {
    pipePluginInformationMap = new ConcurrentHashMap<>();
    pluginToClassMap = new ConcurrentHashMap<>();
  }

  public void addPipePluginInformation(
      String pluginName, PipePluginInformation pipePluginInformation) {
    pipePluginInformationMap.put(pluginName.toUpperCase(), pipePluginInformation);
  }

  public void removePipePluginInformation(String pluginName) {
    pipePluginInformationMap.remove(pluginName.toUpperCase());
  }

  public PipePluginInformation getPipePluginInformation(String pluginName) {
    return pipePluginInformationMap.get(pluginName.toUpperCase());
  }

  public PipePluginInformation[] getAllPipePluginInformation() {
    return pipePluginInformationMap.values().toArray(new PipePluginInformation[0]);
  }

  public boolean containsPipePlugin(String pluginName) {
    return pipePluginInformationMap.containsKey(pluginName.toUpperCase());
  }

  public void addPluginAndClass(String pluginName, Class<?> clazz) {
    pluginToClassMap.put(pluginName.toUpperCase(), clazz);
  }

  public Class<?> getPluginClass(String pluginName) {
    return pluginToClassMap.get(pluginName.toUpperCase());
  }

  public void removePluginClass(String pluginName) {
    pluginToClassMap.remove(pluginName.toUpperCase());
  }

  public void updatePluginClass(
      PipePluginInformation pipePluginInformation, PipePluginClassLoader classLoader)
      throws ClassNotFoundException {
    Class<?> functionClass = Class.forName(pipePluginInformation.getClassName(), true, classLoader);
    pluginToClassMap.put(pipePluginInformation.getPluginName().toUpperCase(), functionClass);
  }

  public void serializePipePluginTable(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(pipePluginInformationMap.size(), outputStream);
    for (PipePluginInformation pipePluginInformation : pipePluginInformationMap.values()) {
      ReadWriteIOUtils.write(pipePluginInformation.serialize(), outputStream);
    }
  }

  public void deserializePipePluginTable(InputStream inputStream) throws IOException {
    int size = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < size; i++) {
      PipePluginInformation pipePluginInformation = PipePluginInformation.deserialize(inputStream);
      pipePluginInformationMap.put(
          pipePluginInformation.getPluginName().toUpperCase(), pipePluginInformation);
    }
  }

  public void clear() {
    pipePluginInformationMap.clear();
  }
}
