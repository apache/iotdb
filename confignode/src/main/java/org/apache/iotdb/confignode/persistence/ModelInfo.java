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

package org.apache.iotdb.confignode.persistence;

import org.apache.iotdb.commons.model.ModelInformation;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@ThreadSafe
public class ModelInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ModelInfo.class);

  private static final String SNAPSHOT_FILENAME = "model_info.snapshot";

  private final Map<String, ModelInformation> modelInfoMap;

  private final ReadWriteLock lock;

  public ModelInfo() {
    this.modelInfoMap = new HashMap<>();
    this.lock = new ReentrantReadWriteLock();
  }

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws TException, IOException {
    File snapshotFile = new File(snapshotDir, SNAPSHOT_FILENAME);
    if (snapshotFile.exists() && snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to take snapshot of ModelInfo, because snapshot file [{}] is already exist.",
          snapshotFile.getAbsolutePath());
      return false;
    }

    lock.readLock().lock();
    try (FileOutputStream fileOutputStream = new FileOutputStream(snapshotFile)) {

      serialize(fileOutputStream);
      return true;
    } finally {
      lock.readLock().unlock();
    }
  }

  private void serialize(FileOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(modelInfoMap.size(), stream);
    for (ModelInformation entry : modelInfoMap.values()) {
      entry.serialize(stream);
    }
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws TException, IOException {
    File snapshotFile = new File(snapshotDir, SNAPSHOT_FILENAME);
    if (!snapshotFile.exists() || !snapshotFile.isFile()) {
      LOGGER.error(
          "Failed to load snapshot of ModelInfo, snapshot file [{}] does not exist.",
          snapshotFile.getAbsolutePath());
      return;
    }
    lock.writeLock().lock();
    try (FileInputStream fileInputStream = new FileInputStream(snapshotFile)) {

      clear();

      deserialize(fileInputStream);

    } finally {
      lock.writeLock().unlock();
    }
  }

  private void clear() {
    modelInfoMap.clear();
  }

  private void deserialize(InputStream stream) throws IOException {
    int size = ReadWriteIOUtils.readInt(stream);
    for (int i = 0; i < size; i++) {
      ModelInformation modelEntry = ModelInformation.deserialize(stream);
      modelInfoMap.put(modelEntry.getModelId(), modelEntry);
    }
  }
}
