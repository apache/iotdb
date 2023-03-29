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
import java.util.Map;

public class TrailInformation {

  public static final String MODEL_PATH = "model_path";

  private final String trailId;
  private final ModelHyperparameter modelHyperparameter;
  private String modelPath;

  public TrailInformation(
      String trailId, ModelHyperparameter modelHyperparameter, String modelPath) {
    this.trailId = trailId;
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

  public String getTrailId() {
    return trailId;
  }

  public ModelHyperparameter getModelHyperparameter() {
    return modelHyperparameter;
  }

  public String getModelPath() {
    return modelPath;
  }

  public ByteBuffer serializeShowTrailResult() throws IOException {
    PublicBAOS buffer = new PublicBAOS();
    DataOutputStream stream = new DataOutputStream(buffer);
    ReadWriteIOUtils.write(trailId, stream);
    ReadWriteIOUtils.write(modelHyperparameter.toString(), stream);
    ReadWriteIOUtils.write(modelPath, stream);
    return ByteBuffer.wrap(buffer.getBuf(), 0, buffer.size());
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(trailId, stream);
    modelHyperparameter.serialize(stream);
    ReadWriteIOUtils.write(modelPath, stream);
  }

  public void serialize(FileOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(trailId, stream);
    modelHyperparameter.serialize(stream);
    ReadWriteIOUtils.write(modelPath, stream);
  }

  public static TrailInformation deserialize(ByteBuffer buffer) {
    return new TrailInformation(
        ReadWriteIOUtils.readString(buffer),
        ModelHyperparameter.deserialize(buffer),
        ReadWriteIOUtils.readString(buffer));
  }

  public static TrailInformation deserialize(InputStream stream) throws IOException {
    return new TrailInformation(
        ReadWriteIOUtils.readString(stream),
        ModelHyperparameter.deserialize(stream),
        ReadWriteIOUtils.readString(stream));
  }
}
