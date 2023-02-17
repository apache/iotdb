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

package org.apache.iotdb.commons.udf.service;

import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class UDFClassLoaderManager implements IService {

  private static final Logger LOGGER = LoggerFactory.getLogger(UDFClassLoaderManager.class);

  private final String libRoot;

  /** The keys in the map are the query IDs of the UDF queries being executed. */
  private final Map<Long, UDFClassLoader> queryIdToUDFClassLoaderMap;

  /**
   * activeClassLoader is used to load all classes under libRoot. libRoot may be updated before the
   * user executes CREATE FUNCTION or after the user executes DROP FUNCTION. Therefore, we need to
   * continuously maintain the activeClassLoader so that the classes it loads are always up-to-date.
   */
  private volatile UDFClassLoader activeClassLoader;

  private UDFClassLoaderManager(String libRoot) {
    this.libRoot = libRoot;
    LOGGER.info("UDF lib root: {}", libRoot);
    queryIdToUDFClassLoaderMap = new ConcurrentHashMap<>();
    activeClassLoader = null;
  }

  public void initializeUDFQuery(long queryId) {
    activeClassLoader.acquire();
    queryIdToUDFClassLoaderMap.put(queryId, activeClassLoader);
  }

  public void finalizeUDFQuery(long queryId) {
    UDFClassLoader classLoader = queryIdToUDFClassLoaderMap.remove(queryId);
    try {
      if (classLoader != null) {
        classLoader.release();
      }
    } catch (IOException e) {
      LOGGER.warn(
          "Failed to close UDFClassLoader (queryId: {}), because {}", queryId, e.toString());
    }
  }

  public UDFClassLoader updateAndGetActiveClassLoader() throws IOException {
    UDFClassLoader deprecatedClassLoader = activeClassLoader;
    activeClassLoader = new UDFClassLoader(libRoot);
    if (deprecatedClassLoader != null) {
      deprecatedClassLoader.markAsDeprecated();
    }
    return activeClassLoader;
  }

  public UDFClassLoader getActiveClassLoader() {
    return activeClassLoader;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // IService
  /////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public void start() throws StartupException {
    try {
      SystemFileFactory.INSTANCE.makeDirIfNecessary(libRoot);
      activeClassLoader = new UDFClassLoader(libRoot);
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
    return ServiceType.UDF_CLASSLOADER_MANAGER_SERVICE;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // singleton instance holder
  /////////////////////////////////////////////////////////////////////////////////////////////////

  private static UDFClassLoaderManager INSTANCE = null;

  public static synchronized UDFClassLoaderManager setupAndGetInstance(String libRoot) {

    if (INSTANCE == null) {
      INSTANCE = new UDFClassLoaderManager(libRoot);
    }
    return INSTANCE;
  }

  public static UDFClassLoaderManager getInstance() {
    return INSTANCE;
  }
}
