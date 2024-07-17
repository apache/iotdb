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
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.consensus.iot.log.GetConsensusReqReaderPlan;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.statemachine.BaseStateMachine;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceManager;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertMultiTabletsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.SearchNode;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.buffer.BloomFilterCache;
import org.apache.iotdb.db.storageengine.buffer.ChunkCache;
import org.apache.iotdb.db.storageengine.buffer.TimeSeriesMetadataCache;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.snapshot.SnapshotLoader;
import org.apache.iotdb.db.storageengine.dataregion.snapshot.SnapshotTaker;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

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
      return new SnapshotTaker(region).takeFullSnapshot(snapshotDir.getAbsolutePath(), true);
    } catch (Exception e) {
      logger.error(
          "Exception occurs when taking snapshot for {}-{} in {}",
          region.getDatabaseName(),
          region.getDataRegionId(),
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
          region.getDataRegionId(),
          snapshotDir,
          e);
      return false;
    }
  }

  @Override
  public void loadSnapshot(File latestSnapshotRootDir) {
    DataRegion newRegion =
        new SnapshotLoader(
                latestSnapshotRootDir.getAbsolutePath(),
                region.getDatabaseName(),
                region.getDataRegionId())
            .loadSnapshotForStateMachine();
    if (newRegion == null) {
      logger.error("Fail to load snapshot from {}", latestSnapshotRootDir);
      return;
    }
    this.region = newRegion;
    try {
      StorageEngine.getInstance()
          .setDataRegion(new DataRegionId(Integer.parseInt(region.getDataRegionId())), region);
      ChunkCache.getInstance().clear();
      TimeSeriesMetadataCache.getInstance().clear();
      BloomFilterCache.getInstance().clear();
    } catch (Exception e) {
      logger.error("Exception occurs when replacing data region in storage engine.", e);
    }
  }

  protected PlanNode grabInsertNode(IndexedConsensusRequest indexedRequest) {
    List<InsertNode> insertNodes = new ArrayList<>(indexedRequest.getRequests().size());
    for (IConsensusRequest req : indexedRequest.getRequests()) {
      // PlanNode in IndexedConsensusRequest should always be InsertNode
      PlanNode planNode = getPlanNode(req);
      if (planNode instanceof SearchNode) {
        ((SearchNode) planNode).setSearchIndex(indexedRequest.getSearchIndex());
      }
      if (planNode instanceof InsertNode) {
        InsertNode innerNode = (InsertNode) planNode;
        insertNodes.add(innerNode);
      } else if (indexedRequest.getRequests().size() == 1) {
        // If the planNode is not InsertNode, it is expected that the IndexedConsensusRequest only
        // contains one request
        return planNode;
      } else {
        throw new IllegalArgumentException(
            "PlanNodes in IndexedConsensusRequest are not InsertNode and "
                + "the size of requests are larger than 1");
      }
    }
    return mergeInsertNodes(insertNodes);
  }

  /**
   * Merge insert nodes sharing same search index ( e.g. tablet-100, tablet-100, tablet-100 will be
   * merged to one multi-tablet). <br>
   * Notice: the continuity of insert nodes sharing same search index should be protected by the
   * upper layer.
   *
   * @exception RuntimeException when insertNodes is empty
   */
  protected InsertNode mergeInsertNodes(List<InsertNode> insertNodes) {
    int size = insertNodes.size();
    if (size == 0) {
      throw new RuntimeException();
    }
    if (size == 1) {
      return insertNodes.get(0);
    }

    InsertNode result;
    List<Integer> index = new ArrayList<>();
    int i = 0;
    switch (insertNodes.get(0).getType()) {
      case INSERT_TABLET:
        // merge to InsertMultiTabletsNode
        List<InsertTabletNode> insertTabletNodes = new ArrayList<>(size);
        for (InsertNode insertNode : insertNodes) {
          insertTabletNodes.add((InsertTabletNode) insertNode);
          index.add(i);
          i++;
        }
        result =
            new InsertMultiTabletsNode(
                insertNodes.get(0).getPlanNodeId(), index, insertTabletNodes);
        break;
      case INSERT_ROW:
        // merge to InsertRowsNode
        List<InsertRowNode> insertRowNodes = new ArrayList<>(size);
        for (InsertNode insertNode : insertNodes) {
          insertRowNodes.add((InsertRowNode) insertNode);
          index.add(i);
          i++;
        }
        result = new InsertRowsNode(insertNodes.get(0).getPlanNodeId(), index, insertRowNodes);
        break;
      case INSERT_ROWS:
        // merge to InsertRowsNode
        List<InsertRowNode> list = new ArrayList<>();
        for (InsertNode insertNode : insertNodes) {
          for (InsertRowNode insertRowNode : ((InsertRowsNode) insertNode).getInsertRowNodeList()) {
            list.add(insertRowNode);
            index.add(i);
            i++;
          }
        }
        result = new InsertRowsNode(insertNodes.get(0).getPlanNodeId(), index, list);
        break;
      default:
        throw new UnSupportedDataTypeException(
            "Unsupported node type " + insertNodes.get(0).getType());
    }
    result.setSearchIndex(insertNodes.get(0).getSearchIndex());
    result.setDevicePath(insertNodes.get(0).getDevicePath());
    return result;
  }

  @Override
  public List<Path> getSnapshotFiles(File latestSnapshotRootDir) {
    try {
      return new SnapshotLoader(
              latestSnapshotRootDir.getAbsolutePath(),
              region.getDatabaseName(),
              region.getDataRegionId())
          .getSnapshotFileInfo().stream().map(File::toPath).collect(Collectors.toList());
    } catch (IOException e) {
      logger.error(
          "Meets error when getting snapshot files for {}-{}",
          region.getDatabaseName(),
          region.getDataRegionId(),
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
        logger.error(e.getMessage());
        return null;
      }
      return QUERY_INSTANCE_MANAGER.execDataQueryFragmentInstance(fragmentInstance, region);
    }
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
              + region.getDataRegionId();
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
