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

package org.apache.iotdb.commons.model;

import org.apache.iotdb.common.rpc.thrift.TaskType;
import org.apache.iotdb.common.rpc.thrift.TrainingState;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.Nullable;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.commons.model.TrailInformation.MODEL_PATH;

public abstract class ModelInformation {

  private final String modelId;

  private final Map<String, String> options;
  private final String datasetFetchSQL;

  private TrainingState trainingState;

  @Nullable private String bestTrailId;
  private final Map<String, TrailInformation> trailMap;

  public static final String TASK_TYPE = "task_type";
  public static final String MODEL_TYPE = "model_type";

  protected ModelInformation(String modelId, Map<String, String> options, String datasetFetchSQL) {
    this.modelId = modelId;
    this.options = options;
    this.datasetFetchSQL = datasetFetchSQL;
    this.trainingState = TrainingState.PENDING;
    this.trailMap = new HashMap<>();
  }

  protected ModelInformation(ByteBuffer buffer) {
    this.modelId = ReadWriteIOUtils.readString(buffer);

    this.options = ReadWriteIOUtils.readMap(buffer);
    this.datasetFetchSQL = ReadWriteIOUtils.readString(buffer);

    this.trainingState = TrainingState.findByValue(ReadWriteIOUtils.readInt(buffer));

    byte isNull = ReadWriteIOUtils.readByte(buffer);
    if (isNull == 1) {
      this.bestTrailId = ReadWriteIOUtils.readString(buffer);
    }

    int mapSize = ReadWriteIOUtils.readInt(buffer);
    this.trailMap = new HashMap<>();
    for (int i = 0; i < mapSize; i++) {
      TrailInformation trailInformation = TrailInformation.deserialize(buffer);
      this.trailMap.put(trailInformation.getTrailId(), trailInformation);
    }
  }

  protected ModelInformation(InputStream stream) throws IOException {
    this.modelId = ReadWriteIOUtils.readString(stream);

    this.options = ReadWriteIOUtils.readMap(stream);
    this.datasetFetchSQL = ReadWriteIOUtils.readString(stream);

    this.trainingState = TrainingState.findByValue(ReadWriteIOUtils.readInt(stream));

    byte isNull = ReadWriteIOUtils.readByte(stream);
    if (isNull == 1) {
      this.bestTrailId = ReadWriteIOUtils.readString(stream);
    }

    int mapSize = ReadWriteIOUtils.readInt(stream);
    this.trailMap = new HashMap<>();
    for (int i = 0; i < mapSize; i++) {
      TrailInformation trailInformation = TrailInformation.deserialize(stream);
      this.trailMap.put(trailInformation.getTrailId(), trailInformation);
    }
  }

  public abstract TaskType getTaskType();

  public String getModelId() {
    return modelId;
  }

  private String getModelType() {
    return options.get(MODEL_TYPE);
  }

  public Map<String, String> getOptions() {
    return options;
  }

  public String getDatasetFetchSQL() {
    return datasetFetchSQL;
  }

  public boolean available() {
    return trainingState == TrainingState.FINISHED;
  }

  public TrailInformation getTrailInformationById(String trailId) {
    if (trailMap.containsKey(trailId)) {
      return trailMap.get(trailId);
    }
    return null;
  }

  public List<TrailInformation> getAllTrailInformation() {
    return new ArrayList<>(trailMap.values());
  }

  public void update(String trailId, Map<String, String> modelInfo) {
    if (!trailMap.containsKey(trailId)) {
      String modelPath = null;
      if (modelInfo.containsKey(MODEL_PATH)) {
        modelPath = modelInfo.get(MODEL_PATH);
        modelInfo.remove(MODEL_PATH);
      }
      TrailInformation trailInformation =
          new TrailInformation(trailId, new ModelHyperparameter(modelInfo), modelPath);
      trailMap.put(trailId, trailInformation);
    } else {
      trailMap.get(trailId).update(modelInfo);
    }
  }

  public void updateState(TrainingState newState, String bestTrailId) {
    // TODO: add state transform validate
    this.trainingState = newState;
    if (bestTrailId != null) {
      this.bestTrailId = bestTrailId;
    }
  }

  public String getModelPath() {
    if (bestTrailId != null) {
      TrailInformation bestTrail = trailMap.get(bestTrailId);
      return bestTrail.getModelPath();
    } else {
      return "UNKNOWN";
    }
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(modelId, stream);

    ReadWriteIOUtils.write(options, stream);
    ReadWriteIOUtils.write(datasetFetchSQL, stream);

    ReadWriteIOUtils.write(trainingState.ordinal(), stream);

    if (bestTrailId == null) {
      ReadWriteIOUtils.write((byte) 0, stream);
    } else {
      ReadWriteIOUtils.write((byte) 1, stream);
      ReadWriteIOUtils.write(bestTrailId, stream);
    }

    ReadWriteIOUtils.write(trailMap.size(), stream);
    for (TrailInformation trailInformation : trailMap.values()) {
      trailInformation.serialize(stream);
    }
  }

  public void serialize(FileOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(modelId, stream);

    ReadWriteIOUtils.write(options, stream);
    ReadWriteIOUtils.write(datasetFetchSQL, stream);

    ReadWriteIOUtils.write(trainingState.ordinal(), stream);

    if (bestTrailId == null) {
      ReadWriteIOUtils.write((byte) 0, stream);
    } else {
      ReadWriteIOUtils.write((byte) 1, stream);
      ReadWriteIOUtils.write(bestTrailId, stream);
    }

    ReadWriteIOUtils.write(trailMap.size(), stream);
    for (TrailInformation trailInformation : trailMap.values()) {
      trailInformation.serialize(stream);
    }
  }

  public static ModelInformation deserialize(ByteBuffer buffer) {
    TaskType modelTask = TaskType.findByValue(ReadWriteIOUtils.readInt(buffer));
    if (modelTask == null) {
      throw new IllegalArgumentException();
    }

    if (modelTask == TaskType.FORECAST) {
      return new ForecastModeInformation(buffer);
    }
    throw new IllegalArgumentException("Invalid task type: " + modelTask);
  }

  public static ModelInformation deserialize(InputStream stream) throws IOException {
    TaskType modelTask = TaskType.findByValue(ReadWriteIOUtils.readInt(stream));
    if (modelTask == null) {
      throw new IllegalArgumentException();
    }

    if (modelTask == TaskType.FORECAST) {
      return new ForecastModeInformation(stream);
    }
    throw new IllegalArgumentException("Invalid task type: " + modelTask);
  }

  public ByteBuffer serializeShowModelResult() throws IOException {
    PublicBAOS buffer = new PublicBAOS();
    DataOutputStream stream = new DataOutputStream(buffer);
    ReadWriteIOUtils.write(modelId, stream);
    ReadWriteIOUtils.write(getTaskType().toString(), stream);
    ReadWriteIOUtils.write(getModelType(), stream);
    ReadWriteIOUtils.write(datasetFetchSQL, stream);
    ReadWriteIOUtils.write(trainingState.toString(), stream);

    if (bestTrailId != null) {
      TrailInformation bestTrail = trailMap.get(bestTrailId);
      List<String> modelHyperparameterList = bestTrail.getModelHyperparameter().toStringList();
      ReadWriteIOUtils.write(modelHyperparameterList.size() + 1, stream);
      for (String hyperparameter : modelHyperparameterList) {
        ReadWriteIOUtils.write(hyperparameter, stream);
      }
      ReadWriteIOUtils.write(" ", stream);
    } else {
      ReadWriteIOUtils.write(1, stream);
      ReadWriteIOUtils.write("UNKNOWN", stream);
    }
    return ByteBuffer.wrap(buffer.getBuf(), 0, buffer.size());
  }
}
