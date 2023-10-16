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

import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class TrialInformation {

  public static final String MODEL_PATH = "model_path";

  private final String trialId;
  private final ModelHyperparameter modelHyperparameter;
  private String modelPath;

  public TrialInformation(
      String trialId, ModelHyperparameter modelHyperparameter, String modelPath) {
    this.trialId = trialId;
    this.modelHyperparameter = modelHyperparameter;
    this.modelPath = modelPath;
  }

  public void update(Map<String, String> modelInfo) {
    if (modelInfo.containsKey(MODEL_PATH)) {
      modelPath = modelInfo.get(MODEL_PATH);
      modelInfo.remove(MODEL_PATH);
    }
    modelHyperparameter.update(modelInfo);
  }

  public String getTrialId() {
    return trialId;
  }

  public ModelHyperparameter getModelHyperparameter() {
    return modelHyperparameter;
  }

  public String getModelPath() {
    return modelPath;
  }

  public ByteBuffer serializeShowTrialResult() throws IOException {
    PublicBAOS buffer = new PublicBAOS();
    DataOutputStream stream = new DataOutputStream(buffer);
    ReadWriteIOUtils.write(trialId, stream);
    ReadWriteIOUtils.write(modelPath, stream);
    List<String> modelHyperparameterList = modelHyperparameter.toStringList();
    ReadWriteIOUtils.write(modelHyperparameterList.size() + 1, stream);
    for (String hyperparameter : modelHyperparameterList) {
      ReadWriteIOUtils.write(hyperparameter, stream);
    }
    // add a line break to make the result more readable.
    ReadWriteIOUtils.write(" ", stream);
    return ByteBuffer.wrap(buffer.getBuf(), 0, buffer.size());
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(trialId, stream);
    modelHyperparameter.serialize(stream);
    ReadWriteIOUtils.write(modelPath, stream);
  }

  public void serialize(FileOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(trialId, stream);
    modelHyperparameter.serialize(stream);
    ReadWriteIOUtils.write(modelPath, stream);
  }

  public static TrialInformation deserialize(ByteBuffer buffer) {
    return new TrialInformation(
        ReadWriteIOUtils.readString(buffer),
        ModelHyperparameter.deserialize(buffer),
        ReadWriteIOUtils.readString(buffer));
  }

  public static TrialInformation deserialize(InputStream stream) throws IOException {
    return new TrialInformation(
        ReadWriteIOUtils.readString(stream),
        ModelHyperparameter.deserialize(stream),
        ReadWriteIOUtils.readString(stream));
  }
}
