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

import org.apache.iotdb.consensus.common.SnapshotMeta;
import org.apache.iotdb.consensus.statemachine.IStateMachine;

import org.apache.ratis.io.MD5Hash;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.SnapshotRetentionPolicy;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.impl.FileListSnapshotInfo;
import org.apache.ratis.util.MD5FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class SnapshotStorage implements StateMachineStorage {
  private IStateMachine applicationStateMachine;
  private File stateMachineDir;
  private Logger logger = LoggerFactory.getLogger(SnapshotStorage.class);

  public SnapshotStorage(IStateMachine applicationStateMachine) {
    this.applicationStateMachine = applicationStateMachine;
  }

  private ByteBuffer getMetadataFromTermIndex(TermIndex termIndex) {
    String ordinal = String.format("%d_%d", termIndex.getTerm(), termIndex.getIndex());
    ByteBuffer metadata = ByteBuffer.wrap(ordinal.getBytes());
    return metadata;
  }

  private TermIndex getTermIndexFromMetadata(ByteBuffer metadata) {
    Charset charset = Charset.defaultCharset();
    CharBuffer charBuffer = charset.decode(metadata);
    String ordinal = charBuffer.toString();
    String[] items = ordinal.split("_");
    return TermIndex.valueOf(Long.parseLong(items[0]), Long.parseLong(items[1]));
  }

  @Override
  public void init(RaftStorage raftStorage) throws IOException {
    this.stateMachineDir = raftStorage.getStorageDir().getStateMachineDir();
  }

  @Override
  public SnapshotInfo getLatestSnapshot() {
    SnapshotMeta snapshotMeta = applicationStateMachine.getLatestSnapshot(stateMachineDir);
    if (snapshotMeta == null) {
      return null;
    }
    TermIndex snapshotTermIndex = getTermIndexFromMetadata(snapshotMeta.getMetadata());

    List<FileInfo> fileInfos = new ArrayList<>();
    try {
      for (File file : snapshotMeta.getSnapshotFiles()) {
        Path filePath = file.toPath();
        MD5Hash fileHash = MD5FileUtil.computeMd5ForFile(file);
        FileInfo fileInfo = new FileInfo(filePath, fileHash);
        fileInfos.add(fileInfo);
      }
    } catch (IOException ioException) {
      logger.error("read file info failed for snapshot file ", ioException);
    }

    return new FileListSnapshotInfo(
        fileInfos, snapshotTermIndex.getTerm(), snapshotTermIndex.getIndex());
  }

  public void takeSnapshot(TermIndex lastApplied) {
    ByteBuffer metadata = getMetadataFromTermIndex(lastApplied);
    applicationStateMachine.takeSnapshot(metadata, stateMachineDir);
  }

  public TermIndex loadSnapshot(SnapshotMeta snapshot) {
    applicationStateMachine.loadSnapshot(snapshot);
    ByteBuffer metadata = snapshot.getMetadata();
    return getTermIndexFromMetadata(metadata);
  }

  @Override
  public void format() throws IOException {}

  @Override
  public void cleanupOldSnapshots(SnapshotRetentionPolicy snapshotRetentionPolicy)
      throws IOException {
    applicationStateMachine.cleanUpOldSnapshots(stateMachineDir);
  }
}
