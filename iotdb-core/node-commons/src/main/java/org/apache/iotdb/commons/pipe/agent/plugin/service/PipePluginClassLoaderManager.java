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

package org.apache.iotdb.commons.pipe.agent.plugin.service;

import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.pipe.api.PipePlugin;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@NotThreadSafe
public class PipePluginClassLoaderManager implements IService {

  private final String libRoot;

  /**
   * Each {@link PipePlugin} is equipped with a dedicated {@link PipePluginClassLoader}. When a
   * {@link PipePlugin} is created, the corresponding {@link PipePluginClassLoader} is generated and
   * used to load the {@link PipePlugin}. When the {@link PipePlugin} is deleted, its associated
   * {@link PipePluginClassLoader} is also removed. The lifecycle of the {@link
   * PipePluginClassLoader} is strictly consistent with the lifecycle of the {@link PipePlugin} it
   * serves.
   */
  private final Map<String, PipePluginClassLoader> pipePluginNameToClassLoaderMap;

  private PipePluginClassLoaderManager(String libRoot) throws IOException {
    this.libRoot = libRoot;
    pipePluginNameToClassLoaderMap = new ConcurrentHashMap<>();
  }

  public void removePluginClassLoader(String pluginName) throws IOException {
    PipePluginClassLoader classLoader =
        pipePluginNameToClassLoaderMap.remove(pluginName.toUpperCase());
    if (classLoader != null) {
      classLoader.markAsDeprecated();
    }
  }

  public PipePluginClassLoader getPluginClassLoader(String pluginName) {
    return pipePluginNameToClassLoaderMap.get(pluginName.toUpperCase());
  }

  public void addPluginAndClassLoader(String pluginName, PipePluginClassLoader classLoader) {
    pipePluginNameToClassLoaderMap.put(pluginName.toUpperCase(), classLoader);
  }

  public PipePluginClassLoader createPipePluginClassLoader(String pluginDirPath)
      throws IOException {
    return new PipePluginClassLoader(pluginDirPath);
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // IService
  /////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public void start() throws StartupException {
    try {
      SystemFileFactory.INSTANCE.makeDirIfNecessary(libRoot);
    } catch (IOException e) {
      throw new StartupException(this.getID().getName(), e.getMessage());
    }
  }

  @Override
  public void stop() {
    // nothing to do
  }

  @Override
  public ServiceType getID() {
    return ServiceType.PIPE_PLUGIN_CLASSLOADER_MANAGER_SERVICE;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // singleton instance holder
  /////////////////////////////////////////////////////////////////////////////////////////////////

  private static PipePluginClassLoaderManager instance = null;

  public static synchronized PipePluginClassLoaderManager setupAndGetInstance(String libRoot)
      throws IOException {
    if (instance == null) {
      instance = new PipePluginClassLoaderManager(libRoot);
    }
    return instance;
  }

  public static PipePluginClassLoaderManager getInstance() {
    return instance;
  }
}
