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
package org.apache.iotdb.confignode.manager.sync;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.consensus.request.read.ShowPipeReq;
import org.apache.iotdb.confignode.consensus.request.write.OperatePipeReq;
import org.apache.iotdb.confignode.consensus.response.PipeInfoResp;
import org.apache.iotdb.confignode.manager.ConsensusManager;
import org.apache.iotdb.confignode.manager.Manager;
import org.apache.iotdb.confignode.persistence.SyncReceiverInfo;
import org.apache.iotdb.consensus.common.response.ConsensusReadResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SyncReceiverManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(SyncReceiverManager.class);
  private final Manager configManager;
  private final SyncReceiverInfo syncReceiverInfo;

  public SyncReceiverManager(Manager manager, SyncReceiverInfo syncReceiverInfo) {
    this.configManager = manager;
    this.syncReceiverInfo = syncReceiverInfo;
  }

  /**
   * Operate pipe
   *
   * @return SUCCESS_STATUS if operate pipe successfully.
   */
  public synchronized TSStatus operatePipe(OperatePipeReq req) {
    return getConsensusManager().write(req).getStatus();
  }

  private ConsensusManager getConsensusManager() {
    return configManager.getConsensusManager();
  }

  public PipeInfoResp showPipe(ShowPipeReq showPipeReq) {
    ConsensusReadResponse readResponse = getConsensusManager().read(showPipeReq);
    return (PipeInfoResp) readResponse.getDataset();
  }
}
