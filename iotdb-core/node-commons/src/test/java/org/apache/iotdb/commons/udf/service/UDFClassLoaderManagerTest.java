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

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class UDFClassLoaderManagerTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testDuplicateQueryInitializationReleasesPreviousClassLoader() throws Exception {
    UDFClassLoaderManager manager = newManager();

    manager.initializeUDFQuery("query");
    UDFClassLoader deprecatedClassLoader = getActiveClassLoader(manager);

    manager.updateAndGetActiveClassLoader();
    UDFClassLoader currentClassLoader = getActiveClassLoader(manager);
    manager.initializeUDFQuery("query");

    Assert.assertEquals(0, getActiveQueriesCount(deprecatedClassLoader));
    Assert.assertEquals(1, getActiveQueriesCount(currentClassLoader));

    manager.finalizeUDFQuery("query");

    Assert.assertEquals(0, getActiveQueriesCount(currentClassLoader));
  }

  @Test
  public void testClassLoaderReleaseDoesNotUnderflow() throws Exception {
    UDFClassLoader classLoader =
        new UDFClassLoader(temporaryFolder.newFolder("udf-lib").getAbsolutePath());

    classLoader.release();
    Assert.assertEquals(0, getActiveQueriesCount(classLoader));

    classLoader.acquire();
    classLoader.release();
    classLoader.release();
    Assert.assertEquals(0, getActiveQueriesCount(classLoader));
  }

  private UDFClassLoaderManager newManager() throws Exception {
    Constructor<UDFClassLoaderManager> constructor =
        UDFClassLoaderManager.class.getDeclaredConstructor(String.class);
    constructor.setAccessible(true);
    UDFClassLoaderManager manager =
        constructor.newInstance(temporaryFolder.newFolder("udf-lib").getAbsolutePath());
    manager.start();
    return manager;
  }

  private UDFClassLoader getActiveClassLoader(UDFClassLoaderManager manager) throws Exception {
    Field activeClassLoaderField =
        UDFClassLoaderManager.class.getDeclaredField("activeClassLoader");
    activeClassLoaderField.setAccessible(true);
    AtomicReference<UDFClassLoader> activeClassLoader =
        (AtomicReference<UDFClassLoader>) activeClassLoaderField.get(manager);
    return activeClassLoader.get();
  }

  private long getActiveQueriesCount(UDFClassLoader classLoader) throws Exception {
    Field activeQueriesCountField = UDFClassLoader.class.getDeclaredField("activeQueriesCount");
    activeQueriesCountField.setAccessible(true);
    return ((AtomicLong) activeQueriesCountField.get(classLoader)).get();
  }
}
