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
package org.apache.iotdb.confignode.consensus.statemachine;

import org.apache.iotdb.confignode.exception.physical.UnknownPhysicalPlanTypeException;
import org.apache.iotdb.confignode.physical.PhysicalPlan;
import org.apache.iotdb.confignode.service.executor.PlanExecutor;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The StandAlone version StateMachine for ConfigNode, mainly used in development */
public class StandAloneStateMachine extends BaseStateMachine {

  private static final Logger LOGGER = LoggerFactory.getLogger(StandAloneStateMachine.class);

  private final PlanExecutor executor = new PlanExecutor();

  /** Transmit PhysicalPlan to confignode.service.executor.PlanExecutor */
  @Override
  protected TSStatus write(PhysicalPlan plan) {
    TSStatus result;
    try {
      result = executor.executorNonQueryPlan(plan);
    } catch (UnknownPhysicalPlanTypeException e) {
      LOGGER.error(e.getMessage());
      result = new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }
    return result;
  }

  /** Transmit PhysicalPlan to confignode.service.executor.PlanExecutor */
  @Override
  protected DataSet read(PhysicalPlan plan) {
    DataSet result;
    try {
      result = executor.executorQueryPlan(plan);
    } catch (UnknownPhysicalPlanTypeException e) {
      LOGGER.error(e.getMessage());
      result = null;
    }
    return result;
  }
}
