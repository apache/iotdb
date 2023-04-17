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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.persistence.pipe.PipeTaskInfo;
import org.apache.iotdb.confignode.rpc.thrift.TCreatePipeReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllPipeInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TRecordPipeMessageReq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PipeTaskCoordinator {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTaskCoordinator.class);

  private final ConfigManager configManager;
  private final PipeTaskInfo pipeTaskInfo;

  public PipeTaskCoordinator(ConfigManager configManager, PipeTaskInfo pipeTaskInfo) {
    this.configManager = configManager;
    this.pipeTaskInfo = pipeTaskInfo;
  }

  public TSStatus createPipe(TCreatePipeReq req) {
    return configManager.getProcedureManager().createPipe(req);
  }

  public TSStatus startPipe(String pipeName) {
    return configManager.getProcedureManager().startPipe(pipeName);
  }

  public TSStatus stopPipe(String pipeName) {
    return configManager.getProcedureManager().stopPipe(pipeName);
  }

  public TSStatus dropPipe(String pipeName) {
    return configManager.getProcedureManager().dropPipe(pipeName);
  }

  public TGetAllPipeInfoResp showPipes() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  public void lockPipeTaskInfo() {
    pipeTaskInfo.acquirePipeTaskInfoLock();
  }

  public void unlockPipeTaskInfo() {
    pipeTaskInfo.releasePipeTaskInfoLock();
  }

  public TSStatus recordPipeMessage(TRecordPipeMessageReq req) {
    throw new UnsupportedOperationException("Not implemented yet");
  }
}
