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

package org.apache.iotdb.commons.pipe.plugin.meta;

import org.apache.iotdb.commons.pipe.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.commons.pipe.plugin.service.PipePluginClassLoader;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DataNodePipePluginMetaKeeper extends PipePluginMetaKeeper {

  private final Map<String, Class<?>> pipePluginNameToClassMap;

  public DataNodePipePluginMetaKeeper() {
    pipePluginNameToClassMap = new ConcurrentHashMap<>();

    loadBuiltInPlugins();
  }

  @Override
  protected void loadBuiltInPlugins() {
    super.loadBuiltInPlugins();

    for (final BuiltinPipePlugin builtinPipePlugin : BuiltinPipePlugin.values()) {
      addPluginAndClass(
          builtinPipePlugin.getPipePluginName(), builtinPipePlugin.getPipePluginClass());
    }
  }

  public void addPluginAndClass(String pluginName, Class<?> clazz) {
    pipePluginNameToClassMap.put(pluginName.toUpperCase(), clazz);
  }

  public Class<?> getPluginClass(String pluginName) {
    return pipePluginNameToClassMap.get(pluginName.toUpperCase());
  }

  public void removePluginClass(String pluginName) {
    pipePluginNameToClassMap.remove(pluginName.toUpperCase());
  }

  public void updatePluginClass(PipePluginMeta pipePluginMeta, PipePluginClassLoader classLoader)
      throws ClassNotFoundException {
    final Class<?> functionClass = Class.forName(pipePluginMeta.getClassName(), true, classLoader);
    pipePluginNameToClassMap.put(pipePluginMeta.getPluginName().toUpperCase(), functionClass);
  }
}
