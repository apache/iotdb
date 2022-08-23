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
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class SnapshotStorage implements StateMachineStorage {
  private final Logger logger = LoggerFactory.getLogger(SnapshotStorage.class);
  private final IStateMachine applicationStateMachine;
  private final String META_FILE_PREFIX = ".ratis_meta.";

  private File stateMachineDir;

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
    moveSnapshotFileToSubDirectory();
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

  private List<Path> getAllFilesUnder(File rootDir) {
    List<Path> allFiles = new ArrayList<>();
    try {
      Files.walkFileTree(
          rootDir.toPath(),
          new FileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
                throws IOException {
              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                throws IOException {
              if (attrs.isRegularFile()) {
                allFiles.add(file);
              }
              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
              logger.info("visit file {} failed due to {}", file.toAbsolutePath(), exc);
              return FileVisitResult.TERMINATE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc)
                throws IOException {
              return FileVisitResult.CONTINUE;
            }
          });
    } catch (IOException ioException) {
      logger.error("IOException occurred during listing snapshot directory: ", ioException);
      return Collections.emptyList();
    }
    return allFiles;
  }

  @Override
  public SnapshotInfo getLatestSnapshot() {
    File latestSnapshotDir = findLatestSnapshotDir();
    if (latestSnapshotDir == null) {
      return null;
    }
    TermIndex snapshotTermIndex = Utils.getTermIndexFromDir(latestSnapshotDir);

    List<FileInfo> fileInfos = new ArrayList<>();
    for (Path file : getAllFilesUnder(latestSnapshotDir)) {
      if (file.endsWith(".md5")) {
        continue;
      }
      FileInfo fileInfo = new FileInfoWithDelayedMd5Computing(file);
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

  private String getMetafileMatcherRegex() {
    // meta file should always end with term_index
    return META_FILE_PREFIX + "\\d+_\\d+$";
  }

  /**
   * After leader InstallSnapshot to a slow follower, Ratis will put all snapshot files directly
   * under statemachineDir. We need to handle this special scenario and rearrange these files to
   * appropriate sub-directory this function will move all snapshot files directly under /sm to
   * /sm/term_index/
   */
  void moveSnapshotFileToSubDirectory() {
    File[] potentialMetafile =
        stateMachineDir.listFiles((dir, name) -> name.matches(getMetafileMatcherRegex()));
    if (potentialMetafile == null || potentialMetafile.length == 0) {
      // the statemachine dir contains no direct metafile
      return;
    }
    String metadata = potentialMetafile[0].getName().substring(META_FILE_PREFIX.length());

    File snapshotDir = getSnapshotDir(metadata);
    snapshotDir.mkdir();

    File[] snapshotFiles = stateMachineDir.listFiles();

    // move files to snapshotDir, if an error occurred, delete snapshotDir
    try {
      if (snapshotFiles == null) {
        logger.error(
            "An unexpected condition triggered. please check implementation "
                + this.getClass().getName());
        FileUtils.deleteFully(snapshotDir);
        return;
      }

      for (File file : snapshotFiles) {
        if (file.equals(snapshotDir)) {
          continue;
        }
        boolean success = file.renameTo(new File(snapshotDir + File.separator + file.getName()));
        if (!success) {
          logger.warn(
              "move snapshot file "
                  + file.getAbsolutePath()
                  + " to sub-directory "
                  + snapshotDir.getAbsolutePath()
                  + "failed");
          FileUtils.deleteFully(snapshotDir);
          break;
        }
      }
    } catch (IOException e) {
      logger.warn("delete directory failed: ", e);
    }
  }
}
