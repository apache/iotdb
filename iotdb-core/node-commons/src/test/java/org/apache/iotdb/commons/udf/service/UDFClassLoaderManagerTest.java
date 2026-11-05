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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.lang.reflect.Field;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class UDFClassLoaderManagerTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testFinalizeWaitsForAllNestedActions() throws Exception {
    UDFClassLoaderManager manager = UDFClassLoaderManager.setupAndGetInstance();
    UDFClassLoaderManager.QueryScope queryScope = manager.openQueryScope("nested-actions");
    UDFClassLoader classLoader = createPinnedDeprecatedClassLoader(queryScope);

    manager.withQueryScope(
        queryScope,
        () -> {
          manager.withQueryScope(
              queryScope,
              () -> {
                manager.finalizeUDFQuery(queryScope);
                return null;
              });
          assertFalse(isClosed(classLoader));
          return null;
        });

    assertTrue(isClosed(classLoader));
  }

  @Test
  public void testFinalizedScopeCannotBeBoundAgain() throws Exception {
    UDFClassLoaderManager manager = UDFClassLoaderManager.setupAndGetInstance();
    UDFClassLoaderManager.QueryScope queryScope = manager.openQueryScope("closed-scope");

    manager.finalizeUDFQuery(queryScope);

    assertThrows(IllegalStateException.class, () -> manager.withQueryScope(queryScope, () -> null));
  }

  private UDFClassLoader createPinnedDeprecatedClassLoader(
      UDFClassLoaderManager.QueryScope queryScope) throws Exception {
    UDFClassLoader classLoader = new UDFClassLoader(temporaryFolder.newFolder().getAbsolutePath());
    assertTrue(classLoader.tryAcquire());
    classLoader.markAsDeprecated();
    getPinnedClassLoaders(queryScope).add(classLoader);
    return classLoader;
  }

  @SuppressWarnings("unchecked")
  private static Set<UDFClassLoader> getPinnedClassLoaders(
      UDFClassLoaderManager.QueryScope queryScope) throws Exception {
    Field field = UDFClassLoaderManager.QueryScope.class.getDeclaredField("pinnedClassLoaders");
    field.setAccessible(true);
    return (Set<UDFClassLoader>) field.get(queryScope);
  }

  private static boolean isClosed(UDFClassLoader classLoader) throws Exception {
    Field field = UDFClassLoader.class.getDeclaredField("closed");
    field.setAccessible(true);
    return field.getBoolean(classLoader);
  }
}
