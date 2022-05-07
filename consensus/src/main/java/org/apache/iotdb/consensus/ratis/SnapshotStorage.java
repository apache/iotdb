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
package org.apache.iotdb.consensus.ratis;

import org.apache.iotdb.consensus.IStateMachine;

import org.apache.ratis.io.MD5Hash;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.SnapshotRetentionPolicy;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.impl.FileListSnapshotInfo;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.MD5FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * TODO: Warning, currently in Ratis 2.2.0, there is a bug in installSnapshot. In subsequent
 * installSnapshot, a follower may fail to install while the leader assume it success. This bug will
 * be triggered when the snapshot threshold is low. This is fixed in current Ratis Master, and
 * hopefully will be introduced in Ratis 2.3.0.
 */
public class SnapshotStorage implements StateMachineStorage {
  private final IStateMachine applicationStateMachine;

  private File stateMachineDir;
  private final Logger logger = LoggerFactory.getLogger(SnapshotStorage.class);

  public SnapshotStorage(IStateMachine applicationStateMachine) {
    this.applicationStateMachine = applicationStateMachine;
  }

  @Override
  public void init(RaftStorage raftStorage) throws IOException {
    this.stateMachineDir = raftStorage.getStorageDir().getStateMachineDir();
  }

  private Path[] getSortedSnapshotDirPaths() {
    ArrayList<Path> snapshotPaths = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(stateMachineDir.toPath())) {
      for (Path path : stream) {
        snapshotPaths.add(path);
      }
    } catch (IOException exception) {
      logger.warn("cannot construct snapshot directory stream ", exception);
      return null;
    }

    Path[] pathArray = snapshotPaths.toArray(new Path[0]);
    Arrays.sort(
        pathArray,
        (o1, o2) -> {
          String index1 = o1.toFile().getName().split("_")[1];
          String index2 = o2.toFile().getName().split("_")[1];
          return Long.compare(Long.parseLong(index1), Long.parseLong(index2));
        });
    return pathArray;
  }

  public File findLatestSnapshotDir() {
    Path[] snapshots = getSortedSnapshotDirPaths();
    if (snapshots == null || snapshots.length == 0) {
      return null;
    }
    return snapshots[snapshots.length - 1].toFile();
  }

  @Override
  public SnapshotInfo getLatestSnapshot() {
    File latestSnapshotDir = findLatestSnapshotDir();
    if (latestSnapshotDir == null) {
      return null;
    }
    TermIndex snapshotTermIndex = Utils.getTermIndexFromDir(latestSnapshotDir);

    List<FileInfo> fileInfos = new ArrayList<>();
    for (File file : Objects.requireNonNull(latestSnapshotDir.listFiles())) {
      Path filePath = file.toPath();
      MD5Hash fileHash = null;
      try {
        fileHash = MD5FileUtil.computeMd5ForFile(file);
      } catch (IOException e) {
        logger.error("read file info failed for snapshot file ", e);
      }
      FileInfo fileInfo = new FileInfo(filePath, fileHash);
      fileInfos.add(fileInfo);
    }

    return new FileListSnapshotInfo(
        fileInfos, snapshotTermIndex.getTerm(), snapshotTermIndex.getIndex());
  }

  @Override
  public void format() throws IOException {}

  @Override
  public void cleanupOldSnapshots(SnapshotRetentionPolicy snapshotRetentionPolicy)
      throws IOException {
    Path[] sortedSnapshotDirs = getSortedSnapshotDirPaths();
    if (sortedSnapshotDirs == null || sortedSnapshotDirs.length == 0) {
      return;
    }
    for (int i = 0; i < sortedSnapshotDirs.length - 1; i++) {
      FileUtils.deleteFully(sortedSnapshotDirs[i]);
    }
  }

  public File getStateMachineDir() {
    return stateMachineDir;
  }

  public File getSnapshotDir(String snapshotMetadata) {
    return new File(stateMachineDir.getAbsolutePath() + File.separator + snapshotMetadata);
  }
}
