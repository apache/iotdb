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

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicLong;

public class PipePluginClassLoaderManagerTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testDuplicatePluginClassLoaderRegistrationClosesPreviousClassLoader()
      throws Exception {
    PipePluginClassLoaderManager manager = newManager();
    TestingPipePluginClassLoader oldClassLoader = newClassLoader();
    TestingPipePluginClassLoader newClassLoader = newClassLoader();

    manager.addPluginAndClassLoader("plugin", oldClassLoader);
    manager.addPluginAndClassLoader("plugin", newClassLoader);

    Assert.assertTrue(oldClassLoader.isClosed());
    Assert.assertSame(newClassLoader, manager.getPluginClassLoader("plugin"));
  }

  @Test
  public void testClassLoaderReleaseDoesNotUnderflow() throws Exception {
    PipePluginClassLoader classLoader = newClassLoader();

    classLoader.release();
    Assert.assertEquals(0, getActiveInstanceCount(classLoader));

    classLoader.acquire();
    classLoader.release();
    classLoader.release();
    Assert.assertEquals(0, getActiveInstanceCount(classLoader));
  }

  private PipePluginClassLoaderManager newManager() throws Exception {
    Constructor<PipePluginClassLoaderManager> constructor =
        PipePluginClassLoaderManager.class.getDeclaredConstructor(String.class);
    constructor.setAccessible(true);
    return constructor.newInstance(temporaryFolder.newFolder("pipe-lib").getAbsolutePath());
  }

  private TestingPipePluginClassLoader newClassLoader() throws Exception {
    return new TestingPipePluginClassLoader(temporaryFolder.newFolder().getAbsolutePath());
  }

  private long getActiveInstanceCount(PipePluginClassLoader classLoader) throws Exception {
    Field activeInstanceCountField =
        PipePluginClassLoader.class.getDeclaredField("activeInstanceCount");
    activeInstanceCountField.setAccessible(true);
    return ((AtomicLong) activeInstanceCountField.get(classLoader)).get();
  }

  private static class TestingPipePluginClassLoader extends PipePluginClassLoader {

    private boolean closed;

    private TestingPipePluginClassLoader(String libRoot) throws Exception {
      super(libRoot);
    }

    @Override
    public void close() throws java.io.IOException {
      closed = true;
      super.close();
    }

    private boolean isClosed() {
      return closed;
    }
  }
}
