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

package org.apache.iotdb.consensus.natraft.protocol.log.applier;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.consensus.natraft.protocol.RaftMember;
import org.apache.iotdb.consensus.natraft.protocol.log.Entry;
import org.apache.iotdb.consensus.natraft.protocol.log.logtype.ConfigChangeEntry;
import org.apache.iotdb.consensus.natraft.protocol.log.logtype.RequestEntry;
import org.apache.iotdb.consensus.natraft.utils.Timer.Statistic;
import org.apache.iotdb.rpc.TSStatusCode;

/** BaseApplier use PlanExecutor to execute PhysicalPlans. */
public class BaseApplier implements LogApplier {

  protected IStateMachine stateMachine;
  protected RaftMember member;

  public BaseApplier(IStateMachine stateMachine, RaftMember member) {
    this.stateMachine = stateMachine;
    this.member = member;
  }

  @TestOnly
  public void setStateMachine(IStateMachine stateMachine) {
    this.stateMachine = stateMachine;
  }

  @Override
  public void apply(Entry e) {

    try {
      if (e instanceof RequestEntry) {
        RequestEntry requestLog = (RequestEntry) e;
        IConsensusRequest request = requestLog.getRequest();
        if (e.createTime > 0) {
          Statistic.LOG_DISPATCHER_FROM_CREATE_TO_APPLYING.calOperationCostTimeFromStart(
              e.createTime);
        }
        if (!member.getConfig().isIgnoreStateMachine()) {
          TSStatus status = applyRequest(request);
          if (status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            e.setException(new ConsensusException(status.message + ":" + status.code));
          }
        }
      } else if (e instanceof ConfigChangeEntry) {
        member.applyConfigChange(((ConfigChangeEntry) e));
      }
    } catch (Exception ex) {
      e.setException(ex);
    } finally {
      e.setApplied(true);
    }
  }

  public TSStatus applyRequest(IConsensusRequest request) {
    request = stateMachine.deserializeRequest(request);
    return stateMachine.write(request);
  }

  @Override
  public IStateMachine getStateMachine() {
    return stateMachine;
  }
}
