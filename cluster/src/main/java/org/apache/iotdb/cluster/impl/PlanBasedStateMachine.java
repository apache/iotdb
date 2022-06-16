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

package org.apache.iotdb.cluster.impl;

import java.io.File;
import org.apache.iotdb.cluster.query.ClusterPlanExecutor;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.cluster.utils.StatusUtils;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PlanBasedStateMachine implements IStateMachine {

  private static final Logger logger = LoggerFactory.getLogger(PlanBasedStateMachine.class);

  private ClusterPlanExecutor planExecutor;
  private MetaGroupMember metaGroupMember;

  public PlanBasedStateMachine() {
  }

  public PlanBasedStateMachine(MetaGroupMember metaGroupMember) {
    this.metaGroupMember = metaGroupMember;
  }

  @Override
  public void start() {
    try {
      planExecutor = new ClusterPlanExecutor(metaGroupMember);
    } catch (QueryProcessException e) {
      logger.error("Cannot initialize plan executor", e);
    }
  }

  @Override
  public void stop() {

  }

  @Override
  public TSStatus write(IConsensusRequest request) {
    if (!(request instanceof PhysicalPlan)) {
      return StatusUtils.getStatus(StatusUtils.EXECUTE_STATEMENT_ERROR,
          "Not supported request: " + request);
    }
    try {
      planExecutor.processNonQuery(((PhysicalPlan) request));
      return StatusUtils.OK;
    } catch (QueryProcessException | StorageGroupNotSetException | StorageEngineException e) {
      logger.warn("Plan execution error", e);
      return StatusUtils.getStatus(StatusUtils.EXECUTE_STATEMENT_ERROR,
          e.getMessage());
    }
  }

  @Override
  public DataSet read(IConsensusRequest request) {
    return null;
  }

  @Override
  public boolean takeSnapshot(File snapshotDir) {
    return false;
  }

  @Override
  public void loadSnapshot(File latestSnapshotRootDir) {

  }

  public void setMetaGroupMember(MetaGroupMember metaGroupMember) {
    this.metaGroupMember = metaGroupMember;
  }
}
