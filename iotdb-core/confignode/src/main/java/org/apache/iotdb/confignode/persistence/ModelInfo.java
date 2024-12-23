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
import org.apache.iotdb.commons.model.ModelStatus;
import org.apache.iotdb.commons.model.ModelTable;
import org.apache.iotdb.commons.model.ModelType;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.confignode.consensus.request.read.model.GetModelInfoPlan;
import org.apache.iotdb.confignode.consensus.request.read.model.ShowModelPlan;
import org.apache.iotdb.confignode.consensus.request.write.model.CreateModelPlan;
import org.apache.iotdb.confignode.consensus.request.write.model.UpdateModelInfoPlan;
import org.apache.iotdb.confignode.consensus.response.model.GetModelInfoResp;
import org.apache.iotdb.confignode.consensus.response.model.ModelTableResp;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@ThreadSafe
public class ModelInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ModelInfo.class);

  private static final String SNAPSHOT_FILENAME = "model_info.snapshot";

  private ModelTable modelTable;

  private final Map<String, List<Integer>> modelNameToNodes;

  private final ReadWriteLock modelTableLock = new ReentrantReadWriteLock();

  private static final Set<String> builtInForecastModel = new HashSet<>();

  private static final Set<String> builtInAnomalyDetectionModel = new HashSet<>();

  static {
    builtInForecastModel.add("_ARIMA");
    builtInForecastModel.add("_NaiveForecaster");
    builtInForecastModel.add("_STLForecaster");
    builtInForecastModel.add("_ExponentialSmoothing");
    builtInAnomalyDetectionModel.add("_GaussianHMM");
    builtInAnomalyDetectionModel.add("_GMMHMM");
    builtInAnomalyDetectionModel.add("_Stray");
  }

  public ModelInfo() {
    this.modelTable = new ModelTable();
    this.modelNameToNodes = new HashMap<>();
  }

  public boolean contain(String modelName) {
    return modelTable.containsModel(modelName);
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

  // init the model in modeInfo, it won't update the details information of the model
  public TSStatus createModel(CreateModelPlan plan) {
    try {
      acquireModelTableWriteLock();
      String modelName = plan.getModelName();
      if (modelTable.containsModel(modelName)) {
        return new TSStatus(TSStatusCode.MODEL_EXIST_ERROR.getStatusCode())
            .setMessage(String.format("model [%s] has already been created.", modelName));
      } else {
        modelTable.addModel(new ModelInformation(modelName, ModelStatus.LOADING));
        return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
      }
    } catch (Exception e) {
      final String errorMessage =
          String.format(
              "Failed to add model [%s] in ModelTable on Config Nodes, because of %s",
              plan.getModelName(), e);
      LOGGER.warn(errorMessage, e);
      return new TSStatus(TSStatusCode.CREATE_MODEL_ERROR.getStatusCode()).setMessage(errorMessage);
    } finally {
      releaseModelTableWriteLock();
    }
  }

  public TSStatus dropModelInNode(int aiNodeId) {
    acquireModelTableWriteLock();
    try {
      for (Map.Entry<String, List<Integer>> entry : modelNameToNodes.entrySet()) {
        entry.getValue().remove(Integer.valueOf(aiNodeId));
        // if list is empty, remove this model totally
        if (entry.getValue().isEmpty()) {
          modelTable.removeModel(entry.getKey());
          modelNameToNodes.remove(entry.getKey());
        }
      }
      // currently, we only have one AINode at a time, so we can just clear failed model.
      modelTable.clearFailedModel();
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } finally {
      releaseModelTableWriteLock();
    }
  }

  public TSStatus dropModel(String modelName) {
    acquireModelTableWriteLock();
    TSStatus status;
    if (modelTable.containsModel(modelName)) {
      modelTable.removeModel(modelName);
      modelNameToNodes.remove(modelName);
      status = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    } else {
      status =
          new TSStatus(TSStatusCode.DROP_MODEL_ERROR.getStatusCode())
              .setMessage(String.format("model [%s] has not been created.", modelName));
    }
    releaseModelTableWriteLock();
    return status;
  }

  public List<Integer> getNodeIds(String modelName) {
    return modelNameToNodes.getOrDefault(modelName, Collections.emptyList());
  }

  private ModelInformation getModelByName(String modelName) {
    ModelType modelType = checkModelType(modelName);
    if (modelType != ModelType.USER_DEFINED) {
      if (modelType == ModelType.BUILT_IN_FORECAST && builtInForecastModel.contains(modelName)) {
        return new ModelInformation(ModelType.BUILT_IN_FORECAST, modelName);
      } else if (modelType == ModelType.BUILT_IN_ANOMALY_DETECTION
          && builtInAnomalyDetectionModel.contains(modelName)) {
        return new ModelInformation(ModelType.BUILT_IN_ANOMALY_DETECTION, modelName);
      }
    } else {
      return modelTable.getModelInformationById(modelName);
    }
    return null;
  }

  public ModelTableResp showModel(ShowModelPlan plan) {
    acquireModelTableReadLock();
    try {
      ModelTableResp modelTableResp =
          new ModelTableResp(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
      if (plan.isSetModelName()) {
        ModelInformation modelInformation = getModelByName(plan.getModelName());
        if (modelInformation != null) {
          modelTableResp.addModelInformation(modelInformation);
        }
      } else {
        modelTableResp.addModelInformation(modelTable.getAllModelInformation());
        for (String modelName : builtInForecastModel) {
          modelTableResp.addModelInformation(
              new ModelInformation(ModelType.BUILT_IN_FORECAST, modelName));
        }
        for (String modelName : builtInAnomalyDetectionModel) {
          modelTableResp.addModelInformation(
              new ModelInformation(ModelType.BUILT_IN_ANOMALY_DETECTION, modelName));
        }
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

  private boolean containsBuiltInModelName(Set<String> builtInModelSet, String modelName) {
    // ignore the case
    for (String builtInModelName : builtInModelSet) {
      if (builtInModelName.equalsIgnoreCase(modelName)) {
        return true;
      }
    }
    return false;
  }

  private ModelType checkModelType(String modelName) {
    if (containsBuiltInModelName(builtInForecastModel, modelName)) {
      return ModelType.BUILT_IN_FORECAST;
    } else if (containsBuiltInModelName(builtInAnomalyDetectionModel, modelName)) {
      return ModelType.BUILT_IN_ANOMALY_DETECTION;
    } else {
      return ModelType.USER_DEFINED;
    }
  }

  private int getAvailableAINodeForModel(String modelName, ModelType modelType) {
    if (modelType == ModelType.USER_DEFINED) {
      List<Integer> aiNodeIds = modelNameToNodes.get(modelName);
      if (aiNodeIds != null) {
        return aiNodeIds.get(0);
      }
    } else {
      // any AINode is fine for built-in model
      // 0 is always the nodeId for configNode, so it's fine to use 0 as special value
      return 0;
    }
    return -1;
  }

  // This method will be used by dataNode to get schema of the model for inference
  public GetModelInfoResp getModelInfo(GetModelInfoPlan plan) {
    acquireModelTableReadLock();
    try {
      String modelName = plan.getModelId();
      GetModelInfoResp getModelInfoResp;
      ModelInformation modelInformation;
      ModelType modelType;
      // check if it's a built-in model
      if ((modelType = checkModelType(modelName)) != ModelType.USER_DEFINED) {
        modelInformation = new ModelInformation(modelType, modelName);
      } else {
        modelInformation = modelTable.getModelInformationById(modelName);
      }

      if (modelInformation != null) {
        getModelInfoResp =
            new GetModelInfoResp(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
      } else {
        TSStatus errorStatus = new TSStatus(TSStatusCode.GET_MODEL_INFO_ERROR.getStatusCode());
        errorStatus.setMessage(String.format("model [%s] has not been created.", modelName));
        getModelInfoResp = new GetModelInfoResp(errorStatus);
        return getModelInfoResp;
      }
      PublicBAOS buffer = new PublicBAOS();
      DataOutputStream stream = new DataOutputStream(buffer);
      modelInformation.serialize(stream);
      getModelInfoResp.setModelInfo(ByteBuffer.wrap(buffer.getBuf(), 0, buffer.size()));
      // select the nodeId to process the task, currently we default use the first one.
      int aiNodeId = getAvailableAINodeForModel(modelName, modelType);
      if (aiNodeId == -1) {
        TSStatus errorStatus = new TSStatus(TSStatusCode.GET_MODEL_INFO_ERROR.getStatusCode());
        errorStatus.setMessage(String.format("There is no AINode with %s available", modelName));
        getModelInfoResp = new GetModelInfoResp(errorStatus);
        return getModelInfoResp;
      } else {
        getModelInfoResp.setTargetAINodeId(aiNodeId);
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
      String modelName = plan.getModelName();
      if (modelTable.containsModel(modelName)) {
        modelTable.updateModel(modelName, plan.getModelInformation());
      }
      if (!plan.getNodeIds().isEmpty()) {
        // only used in model registration, so we can just put the nodeIds in the map without
        // checking
        modelNameToNodes.put(modelName, plan.getNodeIds());
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
      ReadWriteIOUtils.write(modelNameToNodes.size(), fileOutputStream);
      for (Map.Entry<String, List<Integer>> entry : modelNameToNodes.entrySet()) {
        ReadWriteIOUtils.write(entry.getKey(), fileOutputStream);
        ReadWriteIOUtils.write(entry.getValue().size(), fileOutputStream);
        for (Integer nodeId : entry.getValue()) {
          ReadWriteIOUtils.write(nodeId, fileOutputStream);
        }
      }
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
      int size = ReadWriteIOUtils.readInt(fileInputStream);
      for (int i = 0; i < size; i++) {
        String modelName = ReadWriteIOUtils.readString(fileInputStream);
        int nodeSize = ReadWriteIOUtils.readInt(fileInputStream);
        List<Integer> nodes = new LinkedList<>();
        for (int j = 0; j < nodeSize; j++) {
          nodes.add(ReadWriteIOUtils.readInt(fileInputStream));
        }
        modelNameToNodes.put(modelName, nodes);
      }
    } finally {
      releaseModelTableWriteLock();
    }
  }
}
