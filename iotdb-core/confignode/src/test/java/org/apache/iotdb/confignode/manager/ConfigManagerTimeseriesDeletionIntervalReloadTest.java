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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.confignode.manager;

import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Method;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConfigManagerTimeseriesDeletionIntervalReloadTest {
  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();
  private long originalIntervalMs;
  private ConfigManager configManager;
  private ProcedureManager procedureManager;

  @Before
  public void setUp() {
    originalIntervalMs = CONF.getTimeseriesDeletionExecutionIntervalMs();
    configManager = mock(ConfigManager.class);
    procedureManager = mock(ProcedureManager.class);
    when(configManager.getProcedureManager()).thenReturn(procedureManager);
  }

  @After
  public void tearDown() {
    CONF.setTimeseriesDeletionExecutionIntervalMs(originalIntervalMs);
  }

  @Test
  public void testChangedIntervalUpdatesProcedureExecutor() throws Exception {
    final long previousIntervalMs = CONF.getTimeseriesDeletionExecutionIntervalMs();
    final long updatedIntervalMs = previousIntervalMs + 1;
    CONF.setTimeseriesDeletionExecutionIntervalMs(updatedIntervalMs);

    final Method reload =
        ConfigManager.class.getDeclaredMethod(
            "handleTimeseriesDeletionExecutionIntervalHotReload", long.class);
    reload.setAccessible(true);
    reload.invoke(configManager, previousIntervalMs);

    verify(procedureManager).updateTimeseriesDeletionExecutionIntervalMs(updatedIntervalMs);
  }
}
