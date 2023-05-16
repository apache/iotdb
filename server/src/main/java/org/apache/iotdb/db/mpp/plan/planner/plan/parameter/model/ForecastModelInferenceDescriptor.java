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

package org.apache.iotdb.db.mpp.plan.planner.plan.parameter.model;

import org.apache.iotdb.commons.model.ModelInformation;
import org.apache.iotdb.commons.udf.builtin.ModelInferenceFunction;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;

import static org.apache.iotdb.db.constant.SqlConstant.MODEL_ID;
import static org.apache.iotdb.db.constant.SqlConstant.PREDICT_LENGTH;

public class ForecastModelInferenceDescriptor extends ModelInferenceDescriptor {

  private List<TSDataType> inputTypeList;
  private List<Integer> predictIndexList;

  private int modelInputLength;
  private int modelPredictLength;
  private int expectedPredictLength;

  private LinkedHashMap<String, String> outputAttributes;

  public ForecastModelInferenceDescriptor(
      ModelInferenceFunction functionType, ModelInformation modelInformation) {
    super(functionType, modelInformation);
  }

  public ForecastModelInferenceDescriptor(ByteBuffer buffer) {
    super(buffer);
    int listSize = ReadWriteIOUtils.readInt(buffer);
    this.inputTypeList = new ArrayList<>(listSize);
    for (int i = 0; i < listSize; i++) {
      this.inputTypeList.add(TSDataType.deserializeFrom(buffer));
    }
    listSize = ReadWriteIOUtils.readInt(buffer);
    this.predictIndexList = new ArrayList<>(listSize);
    for (int i = 0; i < listSize; i++) {
      this.predictIndexList.add(ReadWriteIOUtils.readInt(buffer));
    }
    this.modelInputLength = ReadWriteIOUtils.readInt(buffer);
    this.modelPredictLength = ReadWriteIOUtils.readInt(buffer);
    this.expectedPredictLength = ReadWriteIOUtils.readInt(buffer);
  }

  public List<Integer> getPredictIndexList() {
    return predictIndexList;
  }

  public void setPredictIndexList(List<Integer> predictIndexList) {
    this.predictIndexList = predictIndexList;
  }

  public List<TSDataType> getInputTypeList() {
    return inputTypeList;
  }

  public void setInputTypeList(List<TSDataType> inputTypeList) {
    this.inputTypeList = inputTypeList;
  }

  public int getModelInputLength() {
    return modelInputLength;
  }

  public int getModelPredictLength() {
    return modelPredictLength;
  }

  public int getExpectedPredictLength() {
    return expectedPredictLength;
  }

  @Override
  public LinkedHashMap<String, String> getOutputAttributes() {
    if (outputAttributes == null) {
      outputAttributes = new LinkedHashMap<>();
      outputAttributes.put(MODEL_ID, modelId);
      if (expectedPredictLength != modelPredictLength) {
        outputAttributes.put(PREDICT_LENGTH, String.valueOf(expectedPredictLength));
      }
    }
    return outputAttributes;
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    super.serialize(byteBuffer);
    ReadWriteIOUtils.write(inputTypeList.size(), byteBuffer);
    for (TSDataType dataType : inputTypeList) {
      dataType.serializeTo(byteBuffer);
    }
    ReadWriteIOUtils.write(predictIndexList.size(), byteBuffer);
    for (Integer index : predictIndexList) {
      ReadWriteIOUtils.write(index, byteBuffer);
    }
    ReadWriteIOUtils.write(modelInputLength, byteBuffer);
    ReadWriteIOUtils.write(modelPredictLength, byteBuffer);
    ReadWriteIOUtils.write(expectedPredictLength, byteBuffer);
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    super.serialize(stream);
    ReadWriteIOUtils.write(inputTypeList.size(), stream);
    for (TSDataType dataType : inputTypeList) {
      dataType.serializeTo(stream);
    }
    ReadWriteIOUtils.write(predictIndexList.size(), stream);
    for (Integer index : predictIndexList) {
      ReadWriteIOUtils.write(index, stream);
    }
    ReadWriteIOUtils.write(modelInputLength, stream);
    ReadWriteIOUtils.write(modelPredictLength, stream);
    ReadWriteIOUtils.write(expectedPredictLength, stream);
  }

  public static ForecastModelInferenceDescriptor deserialize(ByteBuffer buffer) {
    return new ForecastModelInferenceDescriptor(buffer);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    ForecastModelInferenceDescriptor that = (ForecastModelInferenceDescriptor) o;
    return modelInputLength == that.modelInputLength
        && modelPredictLength == that.modelPredictLength
        && expectedPredictLength == that.expectedPredictLength
        && inputTypeList.equals(that.inputTypeList)
        && predictIndexList.equals(that.predictIndexList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        super.hashCode(),
        inputTypeList,
        predictIndexList,
        modelInputLength,
        modelPredictLength,
        expectedPredictLength);
  }
}
