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

package org.apache.iotdb.commons.pipe.plugin.service;

import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;

@NotThreadSafe
public class PipePluginClassLoaderManager implements IService {

  private final String libRoot;

  /**
   * activeClassLoader is used to load all classes under libRoot. libRoot may be updated before the
   * user executes CREATE PIPEPLUGIN or after the user executes DROP PIPEPLUGIN. Therefore, we need
   * to continuously maintain the activeClassLoader so that the classes it loads are always
   * up-to-date.
   */
  private volatile PipePluginClassLoader activeClassLoader;

  private PipePluginClassLoaderManager(String libRoot) throws IOException {
    this.libRoot = libRoot;
    activeClassLoader = new PipePluginClassLoader(libRoot);
  }

  public PipePluginClassLoader updateAndGetActiveClassLoader() throws IOException {
    PipePluginClassLoader deprecatedClassLoader = activeClassLoader;
    activeClassLoader = new PipePluginClassLoader(libRoot);
    if (deprecatedClassLoader != null) {
      deprecatedClassLoader.markAsDeprecated();
    }
    return activeClassLoader;
  }

  public PipePluginClassLoader getActiveClassLoader() {
    return activeClassLoader;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // IService
  /////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public void start() throws StartupException {
    try {
      SystemFileFactory.INSTANCE.makeDirIfNecessary(libRoot);
      activeClassLoader = new PipePluginClassLoader(libRoot);
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

  private static PipePluginClassLoaderManager INSTANCE = null;

  public static synchronized PipePluginClassLoaderManager getInstance(String libRoot)
      throws IOException {
    if (INSTANCE == null) {
      INSTANCE = new PipePluginClassLoaderManager(libRoot);
    }
    return INSTANCE;
  }

  public static PipePluginClassLoaderManager getInstance() {
    return INSTANCE;
  }
}
