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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.model.ModelInformation;
import org.apache.iotdb.commons.model.exception.ModelManagementException;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.confignode.consensus.request.read.model.ShowModelPlan;
import org.apache.iotdb.confignode.consensus.request.read.model.ShowTrailPlan;
import org.apache.iotdb.confignode.consensus.request.write.model.CreateModelPlan;
import org.apache.iotdb.confignode.consensus.request.write.model.DropModelPlan;
import org.apache.iotdb.confignode.consensus.request.write.model.UpdateModelInfoPlan;
import org.apache.iotdb.confignode.consensus.response.ModelTableResp;
import org.apache.iotdb.confignode.consensus.response.TrailTableResp;
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
import java.util.concurrent.locks.ReentrantLock;

@ThreadSafe
public class ModelInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ModelInfo.class);

  private static final String SNAPSHOT_FILENAME = "model_info.snapshot";

  private final Map<String, ModelInformation> modelInfoMap;

  private final ReentrantLock modelTableLock = new ReentrantLock();

  public ModelInfo() {
    this.modelInfoMap = new HashMap<>();
  }

  public void acquireModelTableLock() {
    LOGGER.info("acquire ModelTableLock");
    modelTableLock.lock();
  }

  public void releaseModelTableLock() {
    LOGGER.info("release ModelTableLock");
    modelTableLock.unlock();
  }

  public TSStatus createModel(CreateModelPlan plan) {
    return null;
  }

  public ModelTableResp showModel(ShowModelPlan plan) {
    return null;
  }

  public TrailTableResp showTrail(ShowTrailPlan plan) {
    return null;
  }

  public TSStatus updateModelInfo(UpdateModelInfoPlan plan) {
    return null;
  }

  public TSStatus dropModel(DropModelPlan plan) {
    return null;
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

    acquireModelTableLock();
    try (FileOutputStream fileOutputStream = new FileOutputStream(snapshotFile)) {

      serialize(fileOutputStream);
      return true;
    } finally {
      releaseModelTableLock();
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
    acquireModelTableLock();
    try (FileInputStream fileInputStream = new FileInputStream(snapshotFile)) {

      clear();

      deserialize(fileInputStream);

    } finally {
      releaseModelTableLock();
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

  public void validate(String modelId) {
    if (modelInfoMap.containsKey(modelId)) {
      return;
    }
    throw new ModelManagementException(
        String.format("Failed to drop model [%s], this model has not been created", modelId));
  }
}
