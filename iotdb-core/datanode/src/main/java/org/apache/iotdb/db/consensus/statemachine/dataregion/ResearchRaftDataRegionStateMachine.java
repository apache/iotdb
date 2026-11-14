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
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertMultiTabletsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.buffer.BloomFilterCache;
import org.apache.iotdb.db.storageengine.buffer.ChunkCache;
import org.apache.iotdb.db.storageengine.buffer.TimeSeriesMetadataCache;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.snapshot.SnapshotLoader;
import org.apache.iotdb.db.storageengine.dataregion.snapshot.SnapshotLogger;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/** Self-contained snapshots for the research backends' generic snapshot transfer. */
public class ResearchRaftDataRegionStateMachine extends DataRegionStateMachine {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(ResearchRaftDataRegionStateMachine.class);

  public ResearchRaftDataRegionStateMachine(DataRegion region) {
    super(region);
  }

  /**
   * Writes each maximal run of plain tree-model tablets that share their consensus origin as one
   * InsertMultiTabletsNode: DataRegion inserts its tablets in list order, so the run has the effect
   * of writing them one by one, while the region lock, the rejection check and the per-call write
   * metrics are paid once per run instead of once per tablet. A tablet's failure is reported at its
   * own position; tablets after it in the run are still inserted, as insertTablets always does, so
   * a caller that stops at the first failure must treat the run's later effects as possibly
   * present. Other requests are written one by one.
   */
  @Override
  public List<TSStatus> writeBatch(List<IConsensusRequest> requests) {
    List<TSStatus> statuses = new ArrayList<>(requests.size());
    int start = 0;
    while (start < requests.size()) {
      int end = start + 1;
      while (end < requests.size() && sameTabletRun(requests.get(start), requests.get(end))) {
        end++;
      }
      if (end - start < 2) {
        statuses.add(write(requests.get(start)));
      } else {
        statuses.addAll(writeTabletRun(requests.subList(start, end)));
      }
      start = end;
    }
    return statuses;
  }

  private static boolean sameTabletRun(IConsensusRequest first, IConsensusRequest next) {
    return first.getClass() == InsertTabletNode.class
        && next.getClass() == InsertTabletNode.class
        && ((InsertTabletNode) first).isGeneratedByRemoteConsensusLeader()
            == ((InsertTabletNode) next).isGeneratedByRemoteConsensusLeader();
  }

  private List<TSStatus> writeTabletRun(List<IConsensusRequest> run) {
    List<InsertTabletNode> tablets = new ArrayList<>(run.size());
    List<Integer> positions = new ArrayList<>(run.size());
    for (int i = 0; i < run.size(); i++) {
      tablets.add((InsertTabletNode) run.get(i));
      positions.add(i);
    }
    InsertMultiTabletsNode node =
        new InsertMultiTabletsNode(new PlanNodeId(""), positions, tablets);
    if (tablets.get(0).isGeneratedByRemoteConsensusLeader()) {
      // Leader-written points are counted from this flag of the node DataRegion receives.
      node.markAsGeneratedByRemoteConsensusLeader();
    }
    TSStatus status = write((PlanNode) node);
    List<TSStatus> statuses = new ArrayList<>(run.size());
    boolean success =
        status != null && status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode();
    for (int i = 0; i < run.size(); i++) {
      TSStatus failed = node.getResults().get(i);
      if (failed != null) {
        statuses.add(failed);
      } else if (success || !node.getResults().isEmpty()) {
        statuses.add(StatusUtils.OK);
      } else {
        // A rejection or failure of the whole call (results cleared) belongs to every tablet.
        statuses.add(status);
      }
    }
    return statuses;
  }

  @Override
  public boolean takeSnapshot(File snapshotDir) {
    if (!super.takeSnapshot(snapshotDir)) {
      return false;
    }
    try {
      Path root = snapshotDir.toPath().toAbsolutePath().normalize();
      List<File> files =
          new SnapshotLoader(
                  root.toString(), region.getDatabaseName(), region.getDataRegionIdString())
              .getSnapshotFileInfo();
      for (File file : files) {
        Path source = file.toPath().toAbsolutePath().normalize();
        if (source.startsWith(root)) {
          continue; // Compression-ratio metadata already resides in the snapshot directory.
        }
        Path relative =
            portablePath(source, region.getDatabaseName(), region.getDataRegionIdString());
        Path target = root.resolve(relative);
        Files.createDirectories(target.getParent());
        Files.copy(source, target);
      }
      createRegionDirectories(root);
      // Absolute paths in the original local-only snapshot log must never reach another node.
      Files.delete(root.resolve(SnapshotLogger.SNAPSHOT_LOG_NAME));
      forceSnapshot(root);
      return true;
    } catch (IOException | RuntimeException e) {
      LOGGER.error("Cannot materialize portable DataRegion snapshot {}", snapshotDir, e);
      return false;
    }
  }

  @Override
  public boolean takeSnapshot(File snapshotDir, String snapshotTmpId, String snapshotId) {
    // The consensus engine owns atomic publication of the directory.
    return takeSnapshot(snapshotDir);
  }

  static Path portablePath(Path source, String database, String regionId) throws IOException {
    int size = source.getNameCount();
    if (size < 5) {
      throw new IOException("Invalid snapshot data path: " + source);
    }
    Path relative = source.subpath(size - 5, size);
    String kind = relative.getName(0).toString();
    if ((!IoTDBConstant.SEQUENCE_FOLDER_NAME.equals(kind)
            && !IoTDBConstant.UNSEQUENCE_FOLDER_NAME.equals(kind))
        || !database.equals(relative.getName(1).toString())
        || !regionId.equals(relative.getName(2).toString())) {
      throw new IOException("Snapshot file does not belong to this DataRegion: " + source);
    }
    return relative;
  }

  private void createRegionDirectories(Path root) throws IOException {
    Files.createDirectories(
        root.resolve(IoTDBConstant.SEQUENCE_FOLDER_NAME)
            .resolve(region.getDatabaseName())
            .resolve(region.getDataRegionIdString()));
    Files.createDirectories(
        root.resolve(IoTDBConstant.UNSEQUENCE_FOLDER_NAME)
            .resolve(region.getDatabaseName())
            .resolve(region.getDataRegionIdString()));
  }

  private static void forceSnapshot(Path root) throws IOException {
    List<Path> paths = new ArrayList<>();
    try (Stream<Path> walk = Files.walk(root)) {
      walk.forEach(paths::add);
    }
    // Children are durable before their parent directory entries.
    for (int i = paths.size() - 1; i >= 0; i--) {
      Path path = paths.get(i);
      try (FileChannel channel =
          FileChannel.open(
              path, Files.isDirectory(path) ? StandardOpenOption.READ : StandardOpenOption.WRITE)) {
        channel.force(true);
      }
    }
  }

  @Override
  public List<File> getSnapshotFiles(File snapshotDir) {
    List<File> files = new ArrayList<>();
    try (Stream<Path> walk = Files.walk(snapshotDir.toPath())) {
      walk.filter(Files::isRegularFile).forEach(path -> files.add(path.toFile()));
      return files;
    } catch (IOException e) {
      throw new IllegalStateException("Cannot enumerate portable snapshot " + snapshotDir, e);
    }
  }

  @Override
  public void loadSnapshot(File snapshotDir) {
    try {
      createRegionDirectories(snapshotDir.toPath());
    } catch (IOException e) {
      throw new IllegalStateException("Cannot prepare portable snapshot " + snapshotDir, e);
    }
    DataRegion restored =
        new SnapshotLoader(
                snapshotDir.getAbsolutePath(),
                region.getDatabaseName(),
                region.getDataRegionIdString())
            .loadSnapshotForStateMachine();
    if (restored == null) {
      throw new IllegalStateException("Failed to load portable DataRegion snapshot " + snapshotDir);
    }
    region = restored;
    StorageEngine.getInstance()
        .setDataRegion(new DataRegionId(Integer.parseInt(region.getDataRegionIdString())), region);
    ChunkCache.getInstance().clear();
    TimeSeriesMetadataCache.getInstance().clear();
    BloomFilterCache.getInstance().clear();
  }
}
