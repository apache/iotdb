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

package org.apache.iotdb.confignode.service;

import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;

import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

public class ConfigNodeShutdownHookTest {

  @Test
  public void shouldSkipConsensusShutdownReportWhenConsensusManagerIsNull() throws IOException {
    ConfigNode configNode = Mockito.mock(ConfigNode.class);
    ConfigManager configManager = Mockito.mock(ConfigManager.class);
    Mockito.when(configNode.getConfigManager()).thenReturn(configManager);

    ConfigNodeShutdownHook shutdownHook =
        new ConfigNodeShutdownHook() {
          @Override
          protected ConfigNode getConfigNodeInstance() {
            return configNode;
          }
        };

    shutdownHook.run();
    Mockito.verify(configNode).deactivate();
    Mockito.verify(configManager).getConsensusManager();
  }

  @Test
  public void shouldNotCheckLeadershipBeforeConsensusManagerIsInitialized() throws IOException {
    ConfigNode configNode = Mockito.mock(ConfigNode.class);
    ConfigManager configManager = Mockito.mock(ConfigManager.class);
    ConsensusManager consensusManager = Mockito.mock(ConsensusManager.class);
    Mockito.when(configNode.getConfigManager()).thenReturn(configManager);
    Mockito.when(configManager.getConsensusManager()).thenReturn(consensusManager);
    Mockito.when(consensusManager.isInitialized()).thenReturn(false);

    ConfigNodeShutdownHook shutdownHook =
        new ConfigNodeShutdownHook() {
          @Override
          protected ConfigNode getConfigNodeInstance() {
            return configNode;
          }
        };

    shutdownHook.run();
    Mockito.verify(consensusManager, Mockito.never()).isLeader();
    Mockito.verify(configNode).deactivate();
  }
}
