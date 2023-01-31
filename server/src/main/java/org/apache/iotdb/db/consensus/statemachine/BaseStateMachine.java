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

package org.apache.iotdb.db.consensus.statemachine;

import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.request.ByteBufferConsensusRequest;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.db.mpp.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseStateMachine
    implements IStateMachine, IStateMachine.EventApi, IStateMachine.RetryPolicy {

  private static final Logger logger = LoggerFactory.getLogger(BaseStateMachine.class);

  protected FragmentInstance getFragmentInstance(IConsensusRequest request) {
    FragmentInstance instance;
    if (request instanceof ByteBufferConsensusRequest) {
      instance = FragmentInstance.deserializeFrom(request.serializeToByteBuffer());
    } else if (request instanceof FragmentInstance) {
      instance = (FragmentInstance) request;
    } else {
      logger.error("Unexpected IConsensusRequest : {}", request);
      throw new IllegalArgumentException("Unexpected IConsensusRequest!");
    }
    return instance;
  }

  @Override
  public IConsensusRequest deserializeRequest(IConsensusRequest request) {
    return getPlanNode(request);
  }

  protected PlanNode getPlanNode(IConsensusRequest request) {
    PlanNode node;
    if (request instanceof ByteBufferConsensusRequest) {
      node = PlanNodeType.deserialize(request.serializeToByteBuffer());
    } else if (request instanceof PlanNode) {
      node = (PlanNode) request;
    } else {
      logger.error("Unexpected IConsensusRequest : {}", request);
      throw new IllegalArgumentException("Unexpected IConsensusRequest!");
    }
    return node;
  }
}
