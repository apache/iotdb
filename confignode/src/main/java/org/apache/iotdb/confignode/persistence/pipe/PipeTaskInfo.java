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
package org.apache.iotdb.confignode.persistence.pipe;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.pipe.meta.ConfigNodePipeMetaKeeper;
import org.apache.iotdb.commons.pipe.meta.PipeStatus;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.CreatePipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.DropPipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.SetPipeStatusPlanV2;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.concurrent.locks.ReentrantLock;

public class PipeTaskInfo {

  private final ReentrantLock pipeTaskInfoLock = new ReentrantLock();

  private final ConfigNodePipeMetaKeeper pipeMetaKeeper;

  public PipeTaskInfo(ConfigNodePipeMetaKeeper pipeMetaKeeper) {
    this.pipeMetaKeeper = pipeMetaKeeper;
  }

  /////////////////////////////// Lock ///////////////////////////////

  public void acquirePipeTaskInfoLock() {
    pipeTaskInfoLock.lock();
  }

  public void releasePipeTaskInfoLock() {
    pipeTaskInfoLock.unlock();
  }

  /////////////////////////////// Validator ///////////////////////////////

  public boolean existPipeName(String pipeName) {
    return pipeMetaKeeper.containsPipe(pipeName);
  }

  public PipeStatus getPipeStatus(String pipeName) {
    return pipeMetaKeeper.getPipeMeta(pipeName).getStatus();
  }

  /////////////////////////////// Pipe Task Management ///////////////////////////////

  public TSStatus createPipe(CreatePipePlanV2 plan) {
    pipeMetaKeeper.addPipeMeta(plan.getPipeMeta());
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public TSStatus setPipeStatus(SetPipeStatusPlanV2 plan) {
    pipeMetaKeeper.getPipeMeta(plan.getPipeName()).setStatus(plan.getPipeStatus());
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public TSStatus dropPipe(DropPipePlanV2 plan) {
    pipeMetaKeeper.removePipeMeta(plan.getPipeName());
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }
}
