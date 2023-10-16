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
import org.apache.iotdb.commons.model.TrialInformation;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.confignode.consensus.request.read.model.GetModelInfoPlan;
import org.apache.iotdb.confignode.consensus.request.read.model.ShowModelPlan;
import org.apache.iotdb.confignode.consensus.request.read.model.ShowTrialPlan;
import org.apache.iotdb.confignode.consensus.request.write.model.CreateModelPlan;
import org.apache.iotdb.confignode.consensus.request.write.model.DropModelPlan;
import org.apache.iotdb.confignode.consensus.request.write.model.UpdateModelInfoPlan;
import org.apache.iotdb.confignode.consensus.request.write.model.UpdateModelStatePlan;
import org.apache.iotdb.confignode.consensus.response.model.GetModelInfoResp;
import org.apache.iotdb.confignode.consensus.response.model.ModelTableResp;
import org.apache.iotdb.confignode.consensus.response.model.TrialTableResp;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@ThreadSafe
public class ModelInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ModelInfo.class);

  private static final String SNAPSHOT_FILENAME = "model_info.snapshot";

  private ModelTable modelTable;

  private final ReadWriteLock modelTableLock = new ReentrantReadWriteLock();

  public ModelInfo() {
    this.modelTable = new ModelTable();
  }

  public void acquireModelTableReadLock() {
    LOGGER.info("acquire ModelTableReadLock");
    modelTableLock.readLock().lock();
  }

  public void releaseModelTableReadLock() {
    LOGGER.info("release ModelTableReadLock");
    modelTableLock.readLock().unlock();
  }

  public void acquireModelTableWriteLock() {
    LOGGER.info("acquire ModelTableWriteLock");
    modelTableLock.writeLock().lock();
  }

  public void releaseModelTableWriteLock() {
    LOGGER.info("release ModelTableWriteLock");
    modelTableLock.writeLock().unlock();
  }

  public TSStatus createModel(CreateModelPlan plan) {
    try {
      acquireModelTableWriteLock();
      ModelInformation modelInformation = plan.getModelInformation();
      if (modelTable.containsModel(modelInformation.getModelId())) {
        return new TSStatus(TSStatusCode.MODEL_EXIST_ERROR.getStatusCode())
            .setMessage(
                String.format(
                    "model [%s] has already been created.", modelInformation.getModelId()));
      } else {
        modelTable.addModel(modelInformation);
        return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      }

    } catch (Exception e) {
      final String errorMessage =
          String.format(
              "Failed to add model [%s] in ModelTable on Config Nodes, because of %s",
              plan.getModelInformation().getModelId(), e);
      LOGGER.warn(errorMessage, e);
      return new TSStatus(TSStatusCode.CREATE_MODEL_ERROR.getStatusCode()).setMessage(errorMessage);
    } finally {
      releaseModelTableWriteLock();
    }
  }

  public TSStatus dropModel(DropModelPlan plan) {
    acquireModelTableWriteLock();
    String modelId = plan.getModelId();
    TSStatus status;
    if (modelTable.containsModel(modelId)) {
      modelTable.removeModel(modelId);
      status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } else {
      status =
          new TSStatus(TSStatusCode.DROP_MODEL_ERROR.getStatusCode())
              .setMessage(String.format("model [%s] has not been created.", modelId));
    }
    releaseModelTableWriteLock();
    return status;
  }

  public ModelTableResp showModel(ShowModelPlan plan) {
    acquireModelTableReadLock();
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
      releaseModelTableReadLock();
    }
  }

  public TrialTableResp showTrial(ShowTrialPlan plan) {
    acquireModelTableReadLock();
    try {
      String modelId = plan.getModelId();
      ModelInformation modelInformation = modelTable.getModelInformationById(modelId);
      if (modelInformation == null) {
        return new TrialTableResp(
            new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
                .setMessage(
                    String.format(
                        "Failed to show trails of model [%s], this model has not been created.",
                        modelId)));
      }

      TrialTableResp trialTableResp =
          new TrialTableResp(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
      if (plan.isSetTrialId()) {
        TrialInformation trialInformation =
            modelInformation.getTrialInformationById(plan.getTrialId());
        if (trialInformation != null) {
          trialTableResp.addTrialInformation(trialInformation);
        }
      } else {
        trialTableResp.addTrialInformation(modelInformation.getAllTrialInformation());
      }
      return trialTableResp;
    } catch (IOException e) {
      LOGGER.warn("Fail to get TrailTable", e);
      return new TrialTableResp(
          new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
              .setMessage(e.getMessage()));
    } finally {
      releaseModelTableReadLock();
    }
  }

  public GetModelInfoResp getModelInfo(GetModelInfoPlan plan) {
    acquireModelTableReadLock();
    try {
      GetModelInfoResp getModelInfoResp =
          new GetModelInfoResp(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
      ModelInformation modelInformation = modelTable.getModelInformationById(plan.getModelId());
      if (modelInformation != null) {
        PublicBAOS buffer = new PublicBAOS();
        DataOutputStream stream = new DataOutputStream(buffer);
        modelInformation.serialize(stream);
        getModelInfoResp.setModelInfo(ByteBuffer.wrap(buffer.getBuf(), 0, buffer.size()));
      }
      return getModelInfoResp;
    } catch (IOException e) {
      LOGGER.warn("Fail to get model info", e);
      return new GetModelInfoResp(
          new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode())
              .setMessage(e.getMessage()));
    } finally {
      releaseModelTableReadLock();
    }
  }

  public TSStatus updateModelInfo(UpdateModelInfoPlan plan) {
    acquireModelTableWriteLock();
    try {
      String modelId = plan.getModelId();
      if (modelTable.containsModel(modelId)) {
        modelTable.updateModel(modelId, plan.getTrialId(), plan.getModelInfo());
      }
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } finally {
      releaseModelTableWriteLock();
    }
  }

  public TSStatus updateModelState(UpdateModelStatePlan plan) {
    acquireModelTableWriteLock();
    try {
      String modelId = plan.getModelId();
      if (modelTable.containsModel(modelId)) {
        modelTable.updateState(modelId, plan.getState(), plan.getBestTrialId());
      }
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } finally {
      releaseModelTableWriteLock();
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

    acquireModelTableReadLock();
    try (FileOutputStream fileOutputStream = new FileOutputStream(snapshotFile)) {
      modelTable.serialize(fileOutputStream);
      fileOutputStream.getFD().sync();
      return true;
    } finally {
      releaseModelTableReadLock();
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
    acquireModelTableWriteLock();
    try (FileInputStream fileInputStream = new FileInputStream(snapshotFile)) {
      modelTable.clear();
      modelTable = ModelTable.deserialize(fileInputStream);
    } finally {
      releaseModelTableWriteLock();
    }
  }
}
