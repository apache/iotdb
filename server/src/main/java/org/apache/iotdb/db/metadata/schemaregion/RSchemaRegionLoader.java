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
package org.apache.iotdb.db.metadata.schemaregion;

import org.apache.iotdb.commons.exception.MetadataException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.LinkedList;
import java.util.List;

public class RSchemaRegionLoader {
  private static final Logger LOGGER = LoggerFactory.getLogger(RSchemaRegionLoader.class);
  private static URLClassLoader urlClassLoader = null;
  private static final String RSCHEMA_REGION_CLASS_NAME =
      "org.apache.iotdb.db.metadata.schemaregion.rocksdb.RSchemaRegion";
  private static final String LIB_PATH =
      ".." + File.separator + "lib" + File.separator + "rschema-region" + File.separator;

  public RSchemaRegionLoader() {}

  /**
   * Load the jar files for RSchemaRegion and create an instance of it. The jar files should be
   * located in "../lib/rschema-region". If jar files cannot be found, the function will return
   * null.
   */
  public ISchemaRegion loadRSchemaRegion(ISchemaRegionParams schemaRegionParams)
      throws MetadataException {
    LOGGER.info("Creating instance for schema-engine-rocksdb");
    try {
      loadRSchemaRegionJar();
      Class<?> classForRSchemaRegion = urlClassLoader.loadClass(RSCHEMA_REGION_CLASS_NAME);
      Constructor<?> constructor = classForRSchemaRegion.getConstructor(ISchemaRegionParams.class);
      return (ISchemaRegion) constructor.newInstance(schemaRegionParams);
    } catch (ClassNotFoundException
        | NoSuchMethodException
        | InvocationTargetException
        | InstantiationException
        | IllegalAccessException
        | MalformedURLException
        | RuntimeException e) {
      LOGGER.error("Cannot initialize RSchemaRegion", e);
      throw new MetadataException(e);
    }
  }

  /**
   * Load the jar files for rocksdb and RSchemaRegion. The jar files should be located in directory
   * "../lib/rschema-region". If the jar files have been loaded, it will do nothing.
   */
  private void loadRSchemaRegionJar() throws MalformedURLException {
    LOGGER.info("Loading jar for schema-engine-rocksdb");
    if (urlClassLoader == null) {
      File[] jars = new File(LIB_PATH).listFiles();
      if (jars == null) {
        throw new RuntimeException(
            String.format("Cannot get jars from %s", new File(LIB_PATH).getAbsolutePath()));
      }
      List<URL> dependentJars = new LinkedList<>();
      for (File jar : jars) {
        if (jar.getName().endsWith(".jar")) {
          dependentJars.add(new URL("file:" + jar.getAbsolutePath()));
        }
      }
      urlClassLoader = new URLClassLoader(dependentJars.toArray(new URL[] {}));
    }
  }
}
