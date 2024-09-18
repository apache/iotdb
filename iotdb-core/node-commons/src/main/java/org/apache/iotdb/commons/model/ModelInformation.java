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

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class ModelInformation {

  ModelType modelType;

  private final String modelName;

  private final int[] inputShape;

  private final int[] outputShape;

  private TSDataType[] inputDataType;

  private TSDataType[] outputDataType;

  private ModelStatus status = ModelStatus.INACTIVE;

  String attribute = "";

  public ModelInformation(
      ModelType modelType,
      String modelName,
      int[] inputShape,
      int[] outputShape,
      TSDataType[] inputDataType,
      TSDataType[] outputDataType,
      String attribute,
      ModelStatus status) {
    this.modelType = modelType;
    this.modelName = modelName;
    this.inputShape = inputShape;
    this.outputShape = outputShape;
    this.inputDataType = inputDataType;
    this.outputDataType = outputDataType;
    this.attribute = attribute;
    this.status = status;
  }

  public ModelInformation(
      String modelName,
      int[] inputShape,
      int[] outputShape,
      TSDataType[] inputDataType,
      TSDataType[] outputDataType,
      String attribute) {
    this.modelType = ModelType.USER_DEFINED;
    this.modelName = modelName;
    this.inputShape = inputShape;
    this.outputShape = outputShape;
    this.inputDataType = inputDataType;
    this.outputDataType = outputDataType;
    this.attribute = attribute;
  }

  public ModelInformation(String modelName, ModelStatus status) {
    this.modelType = ModelType.USER_DEFINED;
    this.modelName = modelName;
    this.inputShape = new int[0];
    this.outputShape = new int[0];
    this.outputDataType = new TSDataType[0];
    this.inputDataType = new TSDataType[0];
    this.status = status;
  }

  // init built-in modelInformation
  public ModelInformation(ModelType modelType, String modelName) {
    this.modelType = modelType;
    this.modelName = modelName;
    this.inputShape = new int[2];
    this.outputShape = new int[2];
    this.inputDataType = new TSDataType[0];
    this.outputDataType = new TSDataType[0];
    this.status = ModelStatus.ACTIVE;
  }

  public boolean isBuiltIn() {
    return modelType != ModelType.USER_DEFINED;
  }

  public boolean available() {
    return status == ModelStatus.ACTIVE;
  }

  public void updateStatus(ModelStatus status) {
    this.status = status;
  }

  public String getModelName() {
    return modelName;
  }

  // calculation modelType and outputColumn metadata for different built-in models
  public void setInputColumnSize(int size) {
    inputShape[1] = size;
    if (modelType == ModelType.BUILT_IN_FORECAST) {
      outputShape[1] = size;
    } else if (modelType == ModelType.BUILT_IN_ANOMALY_DETECTION) {
      outputShape[1] = 1;
    }
    if (modelType == ModelType.BUILT_IN_FORECAST) {
      buildOutputDataTypeForBuiltInModel(TSDataType.DOUBLE, outputShape[1]);
    } else if (modelType == ModelType.BUILT_IN_ANOMALY_DETECTION) {
      buildOutputDataTypeForBuiltInModel(TSDataType.INT32, outputShape[1]);
    }
  }

  public void setInputDataType(TSDataType[] inputDataType) {
    this.inputDataType = inputDataType;
  }

  private void buildOutputDataTypeForBuiltInModel(TSDataType tsDataType, int num) {
    outputDataType = new TSDataType[num];
    for (int i = 0; i < num; i++) {
      outputDataType[i] = tsDataType;
    }
  }

  public int[] getInputShape() {
    return inputShape;
  }

  public int[] getOutputShape() {
    return outputShape;
  }

  public TSDataType[] getInputDataType() {
    return inputDataType;
  }

  public TSDataType[] getOutputDataType() {
    return outputDataType;
  }

  public ModelStatus getStatus() {
    return status;
  }

  public String getAttribute() {
    return attribute;
  }

  public void setAttribute(String attribute) {
    this.attribute = attribute;
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(modelType.ordinal(), stream);
    ReadWriteIOUtils.write(status.ordinal(), stream);
    ReadWriteIOUtils.write(modelName, stream);
    if (status == ModelStatus.UNAVAILABLE) {
      return;
    }

    for (int shape : inputShape) {
      ReadWriteIOUtils.write(shape, stream);
    }
    for (int shape : outputShape) {
      ReadWriteIOUtils.write(shape, stream);
    }

    for (TSDataType type : inputDataType) {
      ReadWriteIOUtils.write(type.serialize(), stream);
    }
    for (TSDataType type : outputDataType) {
      ReadWriteIOUtils.write(type.serialize(), stream);
    }

    ReadWriteIOUtils.write(attribute, stream);
  }

  public void serialize(FileOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(modelType.ordinal(), stream);
    ReadWriteIOUtils.write(status.ordinal(), stream);
    ReadWriteIOUtils.write(modelName, stream);
    if (status == ModelStatus.UNAVAILABLE) {
      return;
    }

    for (int shape : inputShape) {
      ReadWriteIOUtils.write(shape, stream);
    }
    for (int shape : outputShape) {
      ReadWriteIOUtils.write(shape, stream);
    }

    for (TSDataType type : inputDataType) {
      ReadWriteIOUtils.write(type.serialize(), stream);
    }
    for (TSDataType type : outputDataType) {
      ReadWriteIOUtils.write(type.serialize(), stream);
    }

    ReadWriteIOUtils.write(attribute, stream);
  }

  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(modelType.ordinal(), byteBuffer);
    ReadWriteIOUtils.write(status.ordinal(), byteBuffer);
    ReadWriteIOUtils.write(modelName, byteBuffer);
    if (status == ModelStatus.UNAVAILABLE) {
      return;
    }

    for (int shape : inputShape) {
      ReadWriteIOUtils.write(shape, byteBuffer);
    }
    for (int shape : outputShape) {
      ReadWriteIOUtils.write(shape, byteBuffer);
    }

    for (TSDataType type : inputDataType) {
      ReadWriteIOUtils.write(type.serialize(), byteBuffer);
    }
    for (TSDataType type : outputDataType) {
      ReadWriteIOUtils.write(type.serialize(), byteBuffer);
    }

    ReadWriteIOUtils.write(attribute, byteBuffer);
  }

  public static ModelInformation deserialize(ByteBuffer buffer) {
    ModelType modelType = ModelType.values()[ReadWriteIOUtils.readInt(buffer)];
    ModelStatus status = ModelStatus.values()[ReadWriteIOUtils.readInt(buffer)];
    String modelName = ReadWriteIOUtils.readString(buffer);
    if (status == ModelStatus.UNAVAILABLE) {
      return new ModelInformation(modelName, status);
    }

    int[] inputShape = new int[2];
    for (int i = 0; i < inputShape.length; i++) {
      inputShape[i] = ReadWriteIOUtils.readInt(buffer);
    }

    int[] outputShape = new int[2];
    for (int i = 0; i < outputShape.length; i++) {
      outputShape[i] = ReadWriteIOUtils.readInt(buffer);
    }

    TSDataType[] inputDataType = new TSDataType[inputShape[1]];
    for (int i = 0; i < inputDataType.length; i++) {
      inputDataType[i] = TSDataType.deserializeFrom(buffer);
    }

    TSDataType[] outputDataType = new TSDataType[outputShape[1]];
    for (int i = 0; i < outputDataType.length; i++) {
      outputDataType[i] = TSDataType.deserializeFrom(buffer);
    }

    String attribute = ReadWriteIOUtils.readString(buffer);

    return new ModelInformation(
        modelType,
        modelName,
        inputShape,
        outputShape,
        inputDataType,
        outputDataType,
        attribute,
        status);
  }

  public static ModelInformation deserialize(InputStream stream) throws IOException {
    ModelType modelType = ModelType.values()[ReadWriteIOUtils.readInt(stream)];
    ModelStatus status = ModelStatus.values()[ReadWriteIOUtils.readInt(stream)];
    String modelName = ReadWriteIOUtils.readString(stream);
    if (status == ModelStatus.UNAVAILABLE) {
      return new ModelInformation(modelName, status);
    }

    int[] inputShape = new int[2];
    for (int i = 0; i < inputShape.length; i++) {
      inputShape[i] = ReadWriteIOUtils.readInt(stream);
    }

    int[] outputShape = new int[2];
    for (int i = 0; i < outputShape.length; i++) {
      outputShape[i] = ReadWriteIOUtils.readInt(stream);
    }

    TSDataType[] inputDataType = new TSDataType[inputShape[1]];
    for (int i = 0; i < inputDataType.length; i++) {
      inputDataType[i] = TSDataType.deserializeFrom(stream);
    }

    TSDataType[] outputDataType = new TSDataType[outputShape[1]];
    for (int i = 0; i < outputDataType.length; i++) {
      outputDataType[i] = TSDataType.deserializeFrom(stream);
    }

    String attribute = ReadWriteIOUtils.readString(stream);
    return new ModelInformation(
        modelType,
        modelName,
        inputShape,
        outputShape,
        inputDataType,
        outputDataType,
        attribute,
        status);
  }

  public ByteBuffer serializeShowModelResult() throws IOException {
    PublicBAOS buffer = new PublicBAOS();
    DataOutputStream stream = new DataOutputStream(buffer);
    ReadWriteIOUtils.write(modelName, stream);
    ReadWriteIOUtils.write(modelType.toString(), stream);
    ReadWriteIOUtils.write(status.toString(), stream);
    ReadWriteIOUtils.write(Arrays.toString(inputShape), stream);
    ReadWriteIOUtils.write(Arrays.toString(outputShape), stream);
    ReadWriteIOUtils.write(Arrays.toString(inputDataType), stream);
    ReadWriteIOUtils.write(Arrays.toString(outputDataType), stream);
    ReadWriteIOUtils.write(attribute, stream);
    // add extra blank line to make the result more readable in cli
    ReadWriteIOUtils.write(" ", stream);
    return ByteBuffer.wrap(buffer.getBuf(), 0, buffer.size());
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ModelInformation) {
      ModelInformation other = (ModelInformation) obj;
      return modelName.equals(other.modelName)
          && modelType.equals(other.modelType)
          && Arrays.equals(inputShape, other.inputShape)
          && Arrays.equals(outputShape, other.outputShape)
          && Arrays.equals(inputDataType, other.inputDataType)
          && Arrays.equals(outputDataType, other.outputDataType)
          && status.equals(other.status)
          && attribute.equals(other.attribute);
    }
    return false;
  }
}
