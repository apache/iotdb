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

import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.request.ByteBufferConsensusRequest;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.statemachine.IStateMachine;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DataRegionStateMachine implements IStateMachine {

  private static final Logger logger = LoggerFactory.getLogger(DataRegionStateMachine.class);

  @Override
  public void start() {}

  @Override
  public void stop() {}

  @Override
  public TSStatus write(IConsensusRequest request) {
    PhysicalPlan plan;
    if (request instanceof ByteBufferConsensusRequest) {
      try {
        plan = PhysicalPlan.Factory.create(((ByteBufferConsensusRequest) request).getContent());
      } catch (IOException | IllegalPathException e) {
        logger.error("Deserialization error for write plan : {}", request);
        return new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
      }
    } else if (request instanceof PhysicalPlan) {
      plan = (PhysicalPlan) request;
    } else {
      logger.error("Unexpected write plan : {}", request);
      return new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }
    logger.info("Execute write plan in DataRegionStateMachine : {}", plan);
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  @Override
  public DataSet read(IConsensusRequest request) {
    logger.info("Execute read plan in DataRegionStateMachine: {}", request);
    return null;
  }
}
