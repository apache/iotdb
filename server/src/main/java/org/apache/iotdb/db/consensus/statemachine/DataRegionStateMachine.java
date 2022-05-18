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
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.db.consensus.statemachine.visitor.DataExecutionVisitor;
import org.apache.iotdb.db.engine.StorageEngineV2;
import org.apache.iotdb.db.engine.snapshot.SnapshotLoader;
import org.apache.iotdb.db.engine.snapshot.SnapshotTaker;
import org.apache.iotdb.db.engine.storagegroup.DataRegion;
import org.apache.iotdb.db.mpp.execution.fragment.FragmentInstanceManager;
import org.apache.iotdb.db.mpp.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class DataRegionStateMachine extends BaseStateMachine {

  private static final Logger logger = LoggerFactory.getLogger(DataRegionStateMachine.class);

  private static final FragmentInstanceManager QUERY_INSTANCE_MANAGER =
      FragmentInstanceManager.getInstance();

  private DataRegion region;

  public DataRegionStateMachine(DataRegion region) {
    this.region = region;
  }

  @Override
  public void start() {}

  @Override
  public void stop() {}

  @Override
  public boolean takeSnapshot(File snapshotDir) {
    try {
      return new SnapshotTaker(region).takeFullSnapshot(snapshotDir.getAbsolutePath(), true);
    } catch (Exception e) {
      logger.error(
          "Exception occurs when taking snapshot for {}-{} in {}",
          region.getLogicalStorageGroupName(),
          region.getDataRegionId(),
          snapshotDir,
          e);
      return false;
    }
  }

  @Override
  public void loadSnapshot(File latestSnapshotRootDir) {
    this.region =
        new SnapshotLoader(
                latestSnapshotRootDir.getAbsolutePath(),
                region.getLogicalStorageGroupName(),
                region.getDataRegionId())
            .loadSnapshotForStateMachine();
    try {
      StorageEngineV2.getInstance()
          .setDataRegion(new DataRegionId(Integer.parseInt(region.getDataRegionId())), region);
    } catch (Exception e) {
      logger.error("Exception occurs when replacing data region in storage engine.", e);
    }
  }

  @Override
  protected TSStatus write(FragmentInstance fragmentInstance) {
    PlanNode planNode = fragmentInstance.getFragment().getRoot();
    return planNode.accept(new DataExecutionVisitor(), region);
  }

  @Override
  protected DataSet read(FragmentInstance fragmentInstance) {
    return QUERY_INSTANCE_MANAGER.execDataQueryFragmentInstance(fragmentInstance, region);
  }
}
