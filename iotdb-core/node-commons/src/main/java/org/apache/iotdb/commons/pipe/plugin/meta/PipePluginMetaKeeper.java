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

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public abstract class PipePluginMetaKeeper {

  protected final Map<String, PipePluginMeta> pipePluginNameToMetaMap = new ConcurrentHashMap<>();
  protected final Map<String, Class<?>> builtinPipePluginNameToClassMap;

  public PipePluginMetaKeeper() {
    builtinPipePluginNameToClassMap = new ConcurrentHashMap<>();
    loadBuiltinPlugins();
  }

  protected void loadBuiltinPlugins() {
    for (final BuiltinPipePlugin builtinPipePlugin : BuiltinPipePlugin.values()) {
      addPipePluginMeta(
          builtinPipePlugin.getPipePluginName(),
          new PipePluginMeta(
              builtinPipePlugin.getPipePluginName(), builtinPipePlugin.getClassName()));
      addBuiltinPluginClass(
          builtinPipePlugin.getPipePluginName(), builtinPipePlugin.getPipePluginClass());
    }
  }

  public void addPipePluginMeta(String pluginName, PipePluginMeta pipePluginMeta) {
    pipePluginNameToMetaMap.put(pluginName.toUpperCase(), pipePluginMeta);
  }

  public void removePipePluginMeta(String pluginName) {
    pipePluginNameToMetaMap.remove(pluginName.toUpperCase());
  }

  public PipePluginMeta getPipePluginMeta(String pluginName) {
    return pipePluginNameToMetaMap.get(pluginName.toUpperCase());
  }

  public PipePluginMeta[] getAllPipePluginMeta() {
    return pipePluginNameToMetaMap.values().toArray(new PipePluginMeta[0]);
  }

  public boolean containsPipePlugin(String pluginName) {
    return pipePluginNameToMetaMap.containsKey(pluginName.toUpperCase());
  }

  private void addBuiltinPluginClass(String pluginName, Class<?> builtinPipePluginClass) {
    builtinPipePluginNameToClassMap.put(pluginName.toUpperCase(), builtinPipePluginClass);
  }

  public Class<?> getBuiltinPluginClass(String pluginName) {
    return builtinPipePluginNameToClassMap.get(pluginName.toUpperCase());
  }

  public String getPluginNameByJarName(String jarName) {
    for (Map.Entry<String, PipePluginMeta> entry : pipePluginNameToMetaMap.entrySet()) {
      if (entry.getValue().getJarName().equals(jarName)) {
        return entry.getKey();
      }
    }
    return null;
  }

  protected void processTakeSnapshot(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(
        (int)
            pipePluginNameToMetaMap.values().stream()
                .filter(pipePluginMeta -> !pipePluginMeta.isBuiltin())
                .count(),
        outputStream);

    for (PipePluginMeta pipePluginMeta : pipePluginNameToMetaMap.values()) {
      if (pipePluginMeta.isBuiltin()) {
        continue;
      }
      ReadWriteIOUtils.write(pipePluginMeta.serialize(), outputStream);
    }
  }

  protected void processLoadSnapshot(InputStream inputStream) throws IOException {
    pipePluginNameToMetaMap.forEach(
        (pluginName, pluginMeta) -> {
          if (!pluginMeta.isBuiltin()) {
            pipePluginNameToMetaMap.remove(pluginName);
          }
        });

    final int pipePluginMetaSize = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < pipePluginMetaSize; i++) {
      final PipePluginMeta pipePluginMeta = PipePluginMeta.deserialize(inputStream);
      addPipePluginMeta(pipePluginMeta.getPluginName().toUpperCase(), pipePluginMeta);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PipePluginMetaKeeper that = (PipePluginMetaKeeper) o;
    return pipePluginNameToMetaMap.equals(that.pipePluginNameToMetaMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pipePluginNameToMetaMap);
  }
}
