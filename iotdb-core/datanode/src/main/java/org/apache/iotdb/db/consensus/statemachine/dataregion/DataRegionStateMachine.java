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

package org.apache.iotdb.db.consensus.statemachine.dataregion;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.consensus.iot.log.GetConsensusReqReaderPlan;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.statemachine.BaseStateMachine;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceManager;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.SearchNode;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.buffer.BloomFilterCache;
import org.apache.iotdb.db.storageengine.buffer.ChunkCache;
import org.apache.iotdb.db.storageengine.buffer.TimeSeriesMetadataCache;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.snapshot.SnapshotLoader;
import org.apache.iotdb.db.storageengine.dataregion.snapshot.SnapshotTaker;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DataRegionStateMachine extends BaseStateMachine {

  private static final Logger logger = LoggerFactory.getLogger(DataRegionStateMachine.class);

  private static final FragmentInstanceManager QUERY_INSTANCE_MANAGER =
      FragmentInstanceManager.getInstance();

  protected DataRegion region;

  private static final int MAX_WRITE_RETRY_TIMES = 5;

  private static final long WRITE_RETRY_WAIT_TIME_IN_MS = 1000;

  public DataRegionStateMachine(DataRegion region) {
    this.region = region;
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
    try {
      SnapshotTaker snapshotTaker = new SnapshotTaker(region);
      snapshotTaker.cleanSnapshot();
      return snapshotTaker.takeFullSnapshot(snapshotDir.getAbsolutePath(), true);
    } catch (Exception e) {
      logger.error(
          "Exception occurs when taking snapshot for {}-{} in {}",
          region.getDatabaseName(),
          region.getDataRegionIdString(),
          snapshotDir,
          e);
      return false;
    }
  }

  @Override
  public boolean takeSnapshot(File snapshotDir, String snapshotTmpId, String snapshotId) {
    try {
      return new SnapshotTaker(region)
          .takeFullSnapshot(snapshotDir.getAbsolutePath(), snapshotTmpId, snapshotId, true);
    } catch (Exception e) {
      logger.error(
          "Exception occurs when taking snapshot for {}-{} in {}",
          region.getDatabaseName(),
          region.getDataRegionIdString(),
          snapshotDir,
          e);
      return false;
    }
  }

  @Override
  public boolean clearSnapshot() {
    return SnapshotTaker.clearSnapshotOfDataRegion(region);
  }

  @Override
  public void loadSnapshot(File latestSnapshotRootDir) {
    DataRegion newRegion =
        new SnapshotLoader(
                latestSnapshotRootDir.getAbsolutePath(),
                region.getDatabaseName(),
                region.getDataRegionIdString())
            .loadSnapshotForStateMachine();
    if (newRegion == null) {
      logger.error("Fail to load snapshot from {}", latestSnapshotRootDir);
      return;
    }
    this.region = newRegion;
    try {
      StorageEngine.getInstance()
          .setDataRegion(
              new DataRegionId(Integer.parseInt(region.getDataRegionIdString())), region);
      ChunkCache.getInstance().clear();
      TimeSeriesMetadataCache.getInstance().clear();
      BloomFilterCache.getInstance().clear();
    } catch (Exception e) {
      logger.error("Exception occurs when replacing data region in storage engine.", e);
    }
  }

  protected PlanNode grabPlanNode(IndexedConsensusRequest indexedRequest) {
    List<SearchNode> searchNodes = new ArrayList<>();
    PlanNode onlyOne = null;
    for (IConsensusRequest req : indexedRequest.getRequests()) {
      // PlanNode in IndexedConsensusRequest should always be InsertNode
      PlanNode planNode = getPlanNode(req);
      if (planNode instanceof SearchNode) {
        ((SearchNode) planNode).setSearchIndex(indexedRequest.getSearchIndex());
        searchNodes.add((SearchNode) planNode);
      } else {
        logger.warn("Unexpected PlanNode type {}, which is not SearchNode", planNode.getClass());
        if (onlyOne == null) {
          onlyOne = planNode;
        } else {
          throw new IllegalArgumentException(
              String.format(
                  "There are two types of PlanNode in one request: %s and %s",
                  onlyOne.getClass(), planNode.getClass()));
        }
      }
    }
    if (onlyOne != null) {
      if (!searchNodes.isEmpty()) {
        throw new IllegalArgumentException(
            String.format(
                "There are two types of PlanNode in one request: %s and SearchNode",
                onlyOne.getClass()));
      }
      return onlyOne;
    }
    // searchNodes should never be empty here
    return searchNodes.get(0).merge(searchNodes);
  }

  @Override
  public List<File> getSnapshotFiles(File latestSnapshotRootDir) {
    try {
      return new SnapshotLoader(
              latestSnapshotRootDir.getAbsolutePath(),
              region.getDatabaseName(),
              region.getDataRegionIdString())
          .getSnapshotFileInfo();
    } catch (IOException e) {
      logger.error(
          "Meets error when getting snapshot files for {}-{}",
          region.getDatabaseName(),
          region.getDataRegionIdString(),
          e);
      return null;
    }
  }

  @Override
  public TSStatus write(IConsensusRequest request) {
    try {
      return write((PlanNode) request);
    } catch (IllegalArgumentException e) {
      logger.error(e.getMessage(), e);
      return new TSStatus(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }
  }

  protected TSStatus write(PlanNode planNode) {
    // To ensure the Data inconsistency between multiple replications, we add retry in write
    // operation.
    TSStatus result = null;
    int retryTime = 0;
    while (retryTime < MAX_WRITE_RETRY_TIMES) {
      result = planNode.accept(new DataExecutionVisitor(), region);
      if (needRetry(result.getCode())) {
        retryTime++;
        logger.debug(
            "write operation failed because {}, retryTime: {}.", result.getCode(), retryTime);
        if (retryTime == MAX_WRITE_RETRY_TIMES) {
          logger.error(
              "write operation still failed after {} retry times, because {}.",
              MAX_WRITE_RETRY_TIMES,
              result.getCode());
        }
        try {
          Thread.sleep(WRITE_RETRY_WAIT_TIME_IN_MS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      } else {
        if (TSStatusCode.TABLE_NOT_EXISTS.getStatusCode() == result.getCode()
            || TSStatusCode.TABLE_IS_LOST.getStatusCode() == result.getCode()) {
          logger.info("table is not exists or lost, result code is {}", result.getCode());
        }
        break;
      }
    }
    return result;
  }

  @Override
  public DataSet read(IConsensusRequest request) {
    if (request instanceof GetConsensusReqReaderPlan) {
      return region.getWALNode().orElseThrow(UnsupportedOperationException::new);
    } else {
      FragmentInstance fragmentInstance;
      try {
        fragmentInstance = getFragmentInstance(request);
      } catch (IllegalArgumentException e) {
        logger.error("Get fragment instance failed", e);
        return null;
      }
      return QUERY_INSTANCE_MANAGER.execDataQueryFragmentInstance(fragmentInstance, region);
    }
  }

  public boolean hasPipeReleaseRegionRelatedResource(ConsensusGroupId groupId) {
    return PipeDataNodeAgent.task().hasPipeReleaseRegionRelatedResource(groupId.getId());
  }

  @Override
  public boolean hasReleaseAllRegionRelatedResource(ConsensusGroupId groupId) {
    boolean releaseAllResource = true;
    releaseAllResource &= hasPipeReleaseRegionRelatedResource(groupId);
    return releaseAllResource;
  }

  @Override
  public File getSnapshotRoot() {
    String snapshotDir = "";
    try {
      snapshotDir =
          IoTDBDescriptor.getInstance().getConfig().getRatisDataRegionSnapshotDir()
              + File.separator
              + region.getDatabaseName()
              + "-"
              + region.getDataRegionIdString();
      return new File(snapshotDir).getCanonicalFile();
    } catch (IOException | NullPointerException e) {
      logger.warn("{}: cannot get the canonical file of {} due to {}", this, snapshotDir, e);
      return null;
    }
  }

  public static boolean needRetry(int statusCode) {
    // To fix the atomicity problem, we only need to add retry for system reject.
    // In other cases, such as readonly, we can return directly because there are retries at the
    // consensus layer.
    return statusCode == TSStatusCode.WRITE_PROCESS_REJECT.getStatusCode();
  }
}
