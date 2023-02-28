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
import org.apache.iotdb.commons.model.ModelTable;
import org.apache.iotdb.commons.model.TrailInformation;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.confignode.consensus.request.read.model.ShowModelPlan;
import org.apache.iotdb.confignode.consensus.request.read.model.ShowTrailPlan;
import org.apache.iotdb.confignode.consensus.request.write.model.CreateModelPlan;
import org.apache.iotdb.confignode.consensus.request.write.model.DropModelPlan;
import org.apache.iotdb.confignode.consensus.request.write.model.UpdateModelInfoPlan;
import org.apache.iotdb.confignode.consensus.request.write.model.UpdateModelStatePlan;
import org.apache.iotdb.confignode.consensus.response.ModelTableResp;
import org.apache.iotdb.confignode.consensus.response.TrailTableResp;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;

@ThreadSafe
public class ModelInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ModelInfo.class);

  private static final String SNAPSHOT_FILENAME = "model_info.snapshot";

  private ModelTable modelTable;

  private final ReentrantLock modelTableLock = new ReentrantLock();

  public ModelInfo() {
    this.modelTable = new ModelTable();
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
    try {
      ModelInformation modelInformation = plan.getModelInformation();
      modelTable.addModel(modelInformation);
    } catch (Exception e) {
      final String errorMessage =
          String.format(
              "Failed to add model [%s] in ModelTable on Config Nodes, because of %s",
              plan.getModelInformation().getModelId(), e);
      LOGGER.warn(errorMessage, e);
      return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
          .setMessage(errorMessage);
    }
    return null;
  }

  public TSStatus dropModel(DropModelPlan plan) {
    String modelId = plan.getModelId();
    if (modelTable.containsModel(modelId)) {
      modelTable.removeModel(modelId);
    }
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  public ModelTableResp showModel(ShowModelPlan plan) {
    acquireModelTableLock();
    try {
      ModelTableResp modelTableResp =
          new ModelTableResp(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
      if (plan.isSetModelId()) {
        ModelInformation modelInformation = modelTable.getModelInformationById(plan.getModelId());
        if (modelInformation != null) {
          modelTableResp.addModelInformation(modelInformation);
        }
      } else {
        modelTableResp.addModelInformation(modelTable.getAllModelInformation());
      }
      return modelTableResp;
    } catch (IOException e) {
      LOGGER.warn("Fail to get ModelTable", e);
      return new ModelTableResp(
          new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
              .setMessage(e.getMessage()));
    } finally {
      releaseModelTableLock();
    }
  }

  public TrailTableResp showTrail(ShowTrailPlan plan) {
    acquireModelTableLock();
    try {
      ModelInformation modelInformation = modelTable.getModelInformationById(plan.getModelId());
      TrailTableResp trailTableResp =
          new TrailTableResp(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
      if (plan.isSetTrailId()) {
        TrailInformation trailInformation =
            modelInformation.getTrailInformationById(plan.getTrailId());
        if (trailInformation != null) {
          trailTableResp.addTrailInformation(trailInformation);
        }
      } else {
        trailTableResp.addTrailInformation(modelInformation.getAllTrailInformation());
      }
      return trailTableResp;
    } catch (IOException e) {
      LOGGER.warn("Fail to get TrailTable", e);
      return new TrailTableResp(
          new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
              .setMessage(e.getMessage()));
    } finally {
      releaseModelTableLock();
    }
  }

  public TSStatus updateModelInfo(UpdateModelInfoPlan plan) {
    acquireModelTableLock();
    try {
      String modelId = plan.getModelId();
      if (modelTable.containsModel(modelId)) {
        modelTable.updateModel(modelId, plan.getTrailId(), plan.getModelInfo());
      }
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } finally {
      releaseModelTableLock();
    }
  }

  public TSStatus updateModelState(UpdateModelStatePlan plan) {
    acquireModelTableLock();
    try {
      String modelId = plan.getModelId();
      if (modelTable.containsModel(modelId)) {
        modelTable.updateState(modelId, plan.getState(), plan.getBestTrailId());
      }
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } finally {
      releaseModelTableLock();
    }
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
      modelTable.serialize(fileOutputStream);
      return true;
    } finally {
      releaseModelTableLock();
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
      modelTable.clear();
      modelTable = ModelTable.deserialize(fileInputStream);
    } finally {
      releaseModelTableLock();
    }
  }

  public boolean isModelExist(String modelId) {
    return modelTable.containsModel(modelId);
  }
}
