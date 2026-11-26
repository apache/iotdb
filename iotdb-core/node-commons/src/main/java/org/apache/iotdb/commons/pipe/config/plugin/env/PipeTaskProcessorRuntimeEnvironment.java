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

package org.apache.iotdb.commons.pipe.config.plugin.env;

import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;

public class PipeTaskProcessorRuntimeEnvironment extends PipeTaskRuntimeEnvironment {

  private final PipeTaskMeta pipeTaskMeta;
  private final UserEntity sourceUserEntity;
  private final String sourcePassword;

  public PipeTaskProcessorRuntimeEnvironment(
      String pipeName, long creationTime, int regionId, PipeTaskMeta pipeTaskMeta) {
    this(pipeName, creationTime, regionId, pipeTaskMeta, null, null);
  }

  public PipeTaskProcessorRuntimeEnvironment(
      String pipeName,
      long creationTime,
      int regionId,
      PipeTaskMeta pipeTaskMeta,
      UserEntity sourceUserEntity,
      String sourcePassword) {
    super(pipeName, creationTime, regionId);
    this.pipeTaskMeta = pipeTaskMeta;
    this.sourceUserEntity = sourceUserEntity;
    this.sourcePassword = sourcePassword;
  }

  public PipeTaskMeta getPipeTaskMeta() {
    return pipeTaskMeta;
  }

  public UserEntity getSourceUserEntity() {
    return sourceUserEntity;
  }

  public String getSourcePassword() {
    return sourcePassword;
  }
}
