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

package org.apache.iotdb.confignode.manager.pipe;

import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.pipe.plugin.PipePluginCoordinator;
import org.apache.iotdb.confignode.manager.pipe.runtime.PipeRuntimeCoordinator;
import org.apache.iotdb.confignode.manager.pipe.task.PipeTaskCoordinator;
import org.apache.iotdb.confignode.persistence.pipe.PipeInfo;

public class PipeManager {

  private final PipePluginCoordinator pipePluginCoordinator;

  private final PipeTaskCoordinator pipeTaskCoordinator;

  private final PipeRuntimeCoordinator pipeRuntimeCoordinator;

  public PipeManager(ConfigManager configManager, PipeInfo pipeInfo) {
    this.pipePluginCoordinator =
        new PipePluginCoordinator(configManager, pipeInfo.getPipePluginInfo());
    this.pipeTaskCoordinator = new PipeTaskCoordinator(configManager, pipeInfo.getPipeTaskInfo());
    this.pipeRuntimeCoordinator = new PipeRuntimeCoordinator(configManager);
  }

  public PipePluginCoordinator getPipePluginCoordinator() {
    return pipePluginCoordinator;
  }

  public PipeTaskCoordinator getPipeTaskCoordinator() {
    return pipeTaskCoordinator;
  }

  public PipeRuntimeCoordinator getPipeRuntimeCoordinator() {
    return pipeRuntimeCoordinator;
  }
}
