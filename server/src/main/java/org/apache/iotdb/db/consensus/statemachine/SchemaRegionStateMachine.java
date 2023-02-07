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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.db.metadata.visitor.SchemaExecutionVisitor;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceManager;
import org.apache.iotdb.db.mpp.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class SchemaRegionStateMachine extends BaseStateMachine {

  private static final Logger logger = LoggerFactory.getLogger(SchemaRegionStateMachine.class);

  private final ISchemaRegion schemaRegion;
  private static final FragmentInstanceManager QUERY_INSTANCE_MANAGER =
      FragmentInstanceManager.getInstance();

  public SchemaRegionStateMachine(ISchemaRegion schemaRegion) {
    this.schemaRegion = schemaRegion;
  }

  @Override
  public void start() {
    // do nothing
  }

  @Override
  public void stop() {
    // do nothing
  }

  @Override
  public boolean isReadOnly() {
    return CommonDescriptor.getInstance().getConfig().isReadOnly();
  }

  @Override
  public boolean takeSnapshot(File snapshotDir) {
    return schemaRegion.createSnapshot(snapshotDir);
  }

  @Override
  public void loadSnapshot(File latestSnapshotRootDir) {
    schemaRegion.loadSnapshot(latestSnapshotRootDir);
  }

  @Override
  public TSStatus write(IConsensusRequest request) {
    try {
      return ((PlanNode) request).accept(new SchemaExecutionVisitor(), schemaRegion);
    } catch (IllegalArgumentException e) {
      logger.error(e.getMessage(), e);
      return new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }
  }

  @Override
  public DataSet read(IConsensusRequest request) {
    FragmentInstance fragmentInstance;
    try {
      fragmentInstance = getFragmentInstance(request);
    } catch (IllegalArgumentException e) {
      logger.error(e.getMessage());
      return null;
    }
    logger.debug(
        "SchemaRegionStateMachine[{}]: Execute read plan: FragmentInstance-{}",
        schemaRegion.getSchemaRegionId(),
        fragmentInstance.getId());
    return QUERY_INSTANCE_MANAGER.execSchemaQueryFragmentInstance(fragmentInstance, schemaRegion);
  }

  @Override
  public boolean shouldRetry(TSStatus writeResult) {
    // TODO implement this
    return super.shouldRetry(writeResult);
  }

  @Override
  public TSStatus updateResult(TSStatus previousResult, TSStatus retryResult) {
    // TODO implement this
    return super.updateResult(previousResult, retryResult);
  }

  @Override
  public long getSleepTime() {
    // TODO implement this
    return super.getSleepTime();
  }
}
