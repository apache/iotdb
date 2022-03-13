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

package org.apache.iotdb.db.consensus.ratis;

import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.statemachine.IStateMachine;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RatisDataRegionStateMachine implements IStateMachine {

  private static final Logger logger = LoggerFactory.getLogger(RatisDataRegionStateMachine.class);

  @Override
  public void start() {}

  @Override
  public void stop() {}

  @Override
  public TSStatus write(IConsensusRequest request) {
    if (request instanceof InsertRowPlan) {
      logger.info("Execute write plan : {}", request);
    }
    return new TSStatus(200);
  }

  @Override
  public DataSet read(IConsensusRequest request) {
    logger.info("Execute read plan : {}", request);
    return null;
  }
}
