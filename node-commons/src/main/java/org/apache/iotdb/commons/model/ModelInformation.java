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

import org.apache.iotdb.common.rpc.thrift.ModelTask;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.commons.model.TrailInformation.MODEL_PATH;

public class ModelInformation {

  private final String modelId;
  private final ModelTask modelTask;
  private final String modelType;

  private final List<String> queryExpressions;
  @Nullable private String queryFilter;

  private final boolean isAuto;
  private TrainingState trainingState;

  @Nullable private String bestTrailId;
  private final Map<String, TrailInformation> trailMap;

  public ModelInformation(
      String modelId,
      ModelTask modelTask,
      String modelType,
      boolean isAuto,
      List<String> queryExpressions,
      @Nullable String queryFilter) {
    this.modelId = modelId;
    this.modelTask = modelTask;
    this.modelType = modelType;
    this.isAuto = isAuto;
    this.trainingState = TrainingState.PENDING;
    this.queryExpressions = queryExpressions;
    this.queryFilter = queryFilter;
    this.trailMap = new HashMap<>();
  }

  public ModelInformation(ByteBuffer buffer) {
    this.modelId = ReadWriteIOUtils.readString(buffer);
    this.modelTask = ModelTask.findByValue(ReadWriteIOUtils.readInt(buffer));
    this.modelType = ReadWriteIOUtils.readString(buffer);

    int listSize = ReadWriteIOUtils.readInt(buffer);
    this.queryExpressions = new ArrayList<>(listSize);
    for (int i = 0; i < listSize; i++) {
      this.queryExpressions.add(ReadWriteIOUtils.readString(buffer));
    }

    byte isNull = ReadWriteIOUtils.readByte(buffer);
    if (isNull == 1) {
      this.queryFilter = ReadWriteIOUtils.readString(buffer);
    }

    this.isAuto = ReadWriteIOUtils.readBool(buffer);
    this.trainingState = TrainingState.findByValue(ReadWriteIOUtils.readInt(buffer));

    isNull = ReadWriteIOUtils.readByte(buffer);
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

  public ModelInformation(InputStream stream) throws IOException {
    this.modelId = ReadWriteIOUtils.readString(stream);
    this.modelTask = ModelTask.findByValue(ReadWriteIOUtils.readInt(stream));
    this.modelType = ReadWriteIOUtils.readString(stream);

    int listSize = ReadWriteIOUtils.readInt(stream);
    this.queryExpressions = new ArrayList<>(listSize);
    for (int i = 0; i < listSize; i++) {
      this.queryExpressions.add(ReadWriteIOUtils.readString(stream));
    }

    byte isNull = ReadWriteIOUtils.readByte(stream);
    if (isNull == 1) {
      this.queryFilter = ReadWriteIOUtils.readString(stream);
    }

    this.isAuto = ReadWriteIOUtils.readBool(stream);
    this.trainingState = TrainingState.findByValue(ReadWriteIOUtils.readInt(stream));

    isNull = ReadWriteIOUtils.readByte(stream);
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

  public String getModelId() {
    return modelId;
  }

  public boolean isAuto() {
    return isAuto;
  }

  public List<String> getQueryExpressions() {
    return queryExpressions;
  }

  @Nullable
  public String getQueryFilter() {
    return queryFilter;
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
    ReadWriteIOUtils.write(modelTask.ordinal(), stream);
    ReadWriteIOUtils.write(modelType, stream);
    ReadWriteIOUtils.write(queryExpressions.size(), stream);
    for (String queryExpression : queryExpressions) {
      ReadWriteIOUtils.write(queryExpression, stream);
    }

    if (queryFilter == null) {
      ReadWriteIOUtils.write((byte) 0, stream);
    } else {
      ReadWriteIOUtils.write((byte) 1, stream);
      ReadWriteIOUtils.write(queryFilter, stream);
    }

    ReadWriteIOUtils.write(isAuto, stream);
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
    ReadWriteIOUtils.write(modelTask.ordinal(), stream);
    ReadWriteIOUtils.write(modelType, stream);

    ReadWriteIOUtils.write(queryExpressions.size(), stream);
    for (String queryExpression : queryExpressions) {
      ReadWriteIOUtils.write(queryExpression, stream);
    }

    if (queryFilter == null) {
      ReadWriteIOUtils.write((byte) 0, stream);
    } else {
      ReadWriteIOUtils.write((byte) 1, stream);
      ReadWriteIOUtils.write(queryFilter, stream);
    }

    ReadWriteIOUtils.write(isAuto, stream);
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

  public static ModelInformation deserialize(InputStream stream) throws IOException {
    return new ModelInformation(stream);
  }

  public static ModelInformation deserialize(ByteBuffer buffer) {
    return new ModelInformation(buffer);
  }

  public ByteBuffer serializeShowModelResult() throws IOException {
    PublicBAOS buffer = new PublicBAOS();
    DataOutputStream stream = new DataOutputStream(buffer);
    ReadWriteIOUtils.write(modelId, stream);
    ReadWriteIOUtils.write(modelTask.toString(), stream);
    ReadWriteIOUtils.write(modelType, stream);
    ReadWriteIOUtils.write(Arrays.toString(queryExpressions.toArray(new String[0])), stream);
    ReadWriteIOUtils.write(trainingState.toString(), stream);

    if (bestTrailId != null) {
      TrailInformation bestTrail = trailMap.get(bestTrailId);
      ReadWriteIOUtils.write(bestTrail.getModelPath(), stream);

      List<String> modelHyperparameterList = bestTrail.getModelHyperparameter().toStringList();
      ReadWriteIOUtils.write(modelHyperparameterList.size(), stream);
      for (String hyperparameter : modelHyperparameterList) {
        ReadWriteIOUtils.write(hyperparameter, stream);
      }
    } else {
      ReadWriteIOUtils.write("UNKNOWN", stream);

      ReadWriteIOUtils.write(1, stream);
      ReadWriteIOUtils.write("UNKNOWN", stream);
    }
    return ByteBuffer.wrap(buffer.getBuf(), 0, buffer.size());
  }
}
