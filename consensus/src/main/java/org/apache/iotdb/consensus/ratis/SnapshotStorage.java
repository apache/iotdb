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

import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.SnapshotRetentionPolicy;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.impl.FileListSnapshotInfo;
import org.apache.ratis.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SnapshotStorage implements StateMachineStorage {
  private final Logger logger = LoggerFactory.getLogger(SnapshotStorage.class);
  private final IStateMachine applicationStateMachine;
  private final String META_FILE_PREFIX = ".ratis_meta.";

  private File stateMachineDir;

  private final ReentrantReadWriteLock snapshotCacheGuard = new ReentrantReadWriteLock();
  private SnapshotInfo currentSnapshot = null;

  public SnapshotStorage(IStateMachine applicationStateMachine) {
    this.applicationStateMachine = applicationStateMachine;
  }

  @Override
  public void init(RaftStorage raftStorage) throws IOException {
    this.stateMachineDir = raftStorage.getStorageDir().getStateMachineDir();
    updateSnapshotCache();
  }

  private Path[] getSortedSnapshotDirPaths() {
    ArrayList<Path> snapshotPaths = new ArrayList<>();
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(stateMachineDir.toPath())) {
      for (Path path : stream) {
        if (path.toFile().isDirectory()) {
          snapshotPaths.add(path);
        }
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
    int i = snapshots.length - 1;
    for (; i >= 0; i--) {
      String metafilePath =
          getMetafilePath(snapshots[i].toFile(), snapshots[i].getFileName().toString());
      if (new File(metafilePath).exists()) {
        break;
      } else {
        try {
          FileUtils.deleteFully(snapshots[i]);
        } catch (IOException e) {
          logger.warn("delete incomplete snapshot directory {} failed due to {}", snapshots[i], e);
        }
      }
    }
    return i < 0 ? null : snapshots[i].toFile();
  }

  SnapshotInfo findLatestSnapshot() {
    File latestSnapshotDir = findLatestSnapshotDir();
    if (latestSnapshotDir == null) {
      return null;
    }
    TermIndex snapshotTermIndex = Utils.getTermIndexFromDir(latestSnapshotDir);

    List<Path> actualSnapshotFiles = applicationStateMachine.getSnapshotFiles(latestSnapshotDir);
    if (actualSnapshotFiles == null) {
      return null;
    }
    Path metaFilePath = Paths.get(getMetafilePath(latestSnapshotDir, latestSnapshotDir.getName()));

    List<FileInfo> fileInfos = new ArrayList<>();
    for (Path file : actualSnapshotFiles) {
      if (file.endsWith(".md5") || file.startsWith(META_FILE_PREFIX)) {
        continue;
      }
      FileInfo fileInfo = new FileInfoWithDelayedMd5Computing(file);
      fileInfos.add(fileInfo);
    }
    // metafile should be sent last for atomicity considerations
    FileInfo metafileInfo = new FileInfoWithDelayedMd5Computing(metaFilePath);
    fileInfos.add(metafileInfo);

    return new FileListSnapshotInfo(
        fileInfos, snapshotTermIndex.getTerm(), snapshotTermIndex.getIndex());
  }

  /*
  Snapshot cache will be updated upon:
  1. takeSnapshot, when RaftServer takes a new snapshot
  2. loadSnapshot, when RaftServer is asked to load a snapshot. loadSnapshot will be called when
  (1) initialize, RaftServer first starts
  (2) reinitialize, RaftServer resumes from pause. Leader will install a snapshot during pause.
   */
  void updateSnapshotCache() {
    snapshotCacheGuard.writeLock().lock();
    try {
      currentSnapshot = findLatestSnapshot();
    } finally {
      snapshotCacheGuard.writeLock().unlock();
    }
  }

  @Override
  public SnapshotInfo getLatestSnapshot() {
    snapshotCacheGuard.readLock().lock();
    try {
      return currentSnapshot;
    } finally {
      snapshotCacheGuard.readLock().unlock();
    }
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

  /**
   * Currently, we name the snapshotDir with Term_Index so that we can tell which directory contains
   * the latest snapshot files. Unfortunately, when leader install snapshot to a slow follower,
   * current Ratis implementation will flatten the directory and place all the snapshots directly
   * under statemachine dir. Under this scenario, we cannot restore Term_Index from directory name.
   * We decided to add an empty metadata file containing only Term_Index into the snapshotDir. his
   * metadata file will be installed along with application snapshot files, so that Term_Index
   * information is kept during InstallSnapshot.
   */
  public boolean addTermIndexMetaFile(File snapshotDir, String termIndexMetadata) {
    File snapshotMetaFile = new File(getMetafilePath(snapshotDir, termIndexMetadata));
    try {
      return snapshotMetaFile.createNewFile();
    } catch (IOException e) {
      logger.warn("cannot create snapshot metafile: ", e);
      return false;
    }
  }

  private String getMetafilePath(File snapshotDir, String termIndexMetadata) {
    // e.g. /_sm/3_39/.ratis_meta.3_39
    return snapshotDir.getAbsolutePath() + File.separator + META_FILE_PREFIX + termIndexMetadata;
  }
}
