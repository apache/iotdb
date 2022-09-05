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

package org.apache.iotdb.commons.trigger.service;

import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class TriggerClassLoaderManager implements IService {
  private static final Logger LOGGER = LoggerFactory.getLogger(TriggerClassLoaderManager.class);

  /** The dir that stores jar files. */
  private final String libRoot;

  /**
   * activeClassLoader is used to load all classes under libRoot. libRoot may be updated before the
   * user executes CREATE TRIGGER or after the user executes DROP TRIGGER. Therefore, we need to
   * continuously maintain the activeClassLoader so that the classes it loads are always up-to-date.
   */
  private volatile TriggerClassLoader activeClassLoader;

  private TriggerClassLoaderManager(String libRoot) {
    this.libRoot = libRoot;
    LOGGER.info("Trigger lib root: {}", libRoot);
    activeClassLoader = null;
  }

  /** Call this method to get up-to-date ClassLoader before registering triggers */
  public TriggerClassLoader updateAndGetActiveClassLoader() throws IOException {
    TriggerClassLoader deprecatedClassLoader = activeClassLoader;
    activeClassLoader = new TriggerClassLoader(libRoot);
    deprecatedClassLoader.close();
    return activeClassLoader;
  }

  public TriggerClassLoader getActiveClassLoader() {
    return activeClassLoader;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // IService
  /////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public void start() throws StartupException {
    try {
      SystemFileFactory.INSTANCE.makeDirIfNecessary(libRoot);
      activeClassLoader = new TriggerClassLoader(libRoot);
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
    return ServiceType.TRIGGER_CLASSLOADER_MANAGER_SERVICE;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // singleton instance holder
  /////////////////////////////////////////////////////////////////////////////////////////////////

  private static TriggerClassLoaderManager INSTANCE = null;

  public static synchronized TriggerClassLoaderManager setupAndGetInstance(String libRoot) {
    if (INSTANCE == null) {
      INSTANCE = new TriggerClassLoaderManager(libRoot);
    }
    return INSTANCE;
  }

  public static TriggerClassLoaderManager getInstance() {
    return INSTANCE;
  }
}
