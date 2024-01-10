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

package org.apache.iotdb.confignode.manager.pipe.transfer.agent.runtime;

import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.persistence.pipe.PipeTaskInfo;
import org.apache.iotdb.confignode.service.ConfigNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

class PipeConfigNodeAgentLauncher {

  private PipeConfigNodeAgentLauncher() {
    // Forbidding instantiation
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeConfigNodeAgentLauncher.class);

  public static synchronized void launchPipeTaskAgent() {
    ConfigManager configManager = ConfigNode.getInstance().getConfigManager();

    final AtomicReference<PipeTaskInfo> pipeTaskInfo =
        configManager.getPipeManager().getPipeTaskCoordinator().tryLock();
    if (pipeTaskInfo == null) {
      LOGGER.warn("Failed to acquire pipe lock for launching pipe task agent.");
      return;
    }
    try {
      pipeTaskInfo.get().handlePipeMetaChangesOnConfigTaskAgent();
    } finally {
      configManager.getPipeManager().getPipeTaskCoordinator().unlock();
    }
  }
}
