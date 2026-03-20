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

package org.apache.iotdb.db.storageengine.dataregion.consistency;

import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 * Persists the partition mutation epochs that the logical consistency layer has already observed.
 */
class LogicalConsistencyPartitionStateStore {

  private static final int FORMAT_VERSION = 1;

  private final Path stateDirOverride;

  LogicalConsistencyPartitionStateStore() {
    this(null);
  }

  LogicalConsistencyPartitionStateStore(Path stateDir) {
    this.stateDirOverride = stateDir;
  }

  public Map<Long, Long> load(String consensusGroupKey) throws IOException {
    Path path = statePath(consensusGroupKey);
    if (!Files.exists(path)) {
      return Collections.emptyMap();
    }

    try (InputStream inputStream = Files.newInputStream(path, StandardOpenOption.READ)) {
      int version = ReadWriteIOUtils.readInt(inputStream);
      if (version != FORMAT_VERSION) {
        throw new IOException("Unsupported logical consistency partition-state format " + version);
      }
      int partitionCount = ReadWriteIOUtils.readInt(inputStream);
      Map<Long, Long> mutationEpochs = new TreeMap<>();
      for (int i = 0; i < partitionCount; i++) {
        mutationEpochs.put(
            ReadWriteIOUtils.readLong(inputStream), ReadWriteIOUtils.readLong(inputStream));
      }
      return mutationEpochs;
    }
  }

  public void persist(String consensusGroupKey, Map<Long, Long> mutationEpochs) throws IOException {
    Path stateDir = getStateDir();
    Files.createDirectories(stateDir);
    Path path = statePath(consensusGroupKey);
    Path tmpPath = path.resolveSibling(path.getFileName() + ".tmp");

    try (FileOutputStream outputStream = new FileOutputStream(tmpPath.toFile())) {
      serialize(mutationEpochs, outputStream);
      outputStream.getFD().sync();
    }

    try {
      Files.move(
          tmpPath, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    } catch (AtomicMoveNotSupportedException e) {
      Files.move(tmpPath, path, StandardCopyOption.REPLACE_EXISTING);
    }
  }

  private Path statePath(String consensusGroupKey) {
    return getStateDir().resolve(encodeFileName(consensusGroupKey) + ".state");
  }

  private Path getStateDir() {
    if (stateDirOverride != null) {
      return stateDirOverride;
    }
    return Paths.get(
        IoTDBDescriptor.getInstance().getConfig().getSystemDir(),
        "consistency-check",
        "partition-state");
  }

  private String encodeFileName(String consensusGroupKey) {
    return Base64.getUrlEncoder()
        .withoutPadding()
        .encodeToString(consensusGroupKey.getBytes(StandardCharsets.UTF_8));
  }

  private void serialize(Map<Long, Long> mutationEpochs, OutputStream outputStream)
      throws IOException {
    ReadWriteIOUtils.write(FORMAT_VERSION, outputStream);
    ReadWriteIOUtils.write(mutationEpochs.size(), outputStream);
    for (Map.Entry<Long, Long> entry : mutationEpochs.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      ReadWriteIOUtils.write(entry.getValue(), outputStream);
    }
  }
}
