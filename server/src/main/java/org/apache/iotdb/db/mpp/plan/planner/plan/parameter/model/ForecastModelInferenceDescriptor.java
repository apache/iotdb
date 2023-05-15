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

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

import static org.apache.iotdb.db.constant.SqlConstant.MODEL_ID;
import static org.apache.iotdb.db.constant.SqlConstant.PREDICT_LENGTH;

public class ForecastModelInferenceDescriptor extends ModelInferenceDescriptor {

  private List<TSDataType> inputTypeList;
  private List<Integer> predictIndexList;

  private int modelInputLength;
  private int modelPredictLength;
  private int expectedPredictLength;

  private String parametersString;
  private LinkedHashMap<String, String> outputAttributes;

  public ForecastModelInferenceDescriptor(
      ModelInferenceFunction functionType, String modelId, ModelInformation modelInformation) {
    super(functionType, modelId);
  }

  public List<Integer> getPredictIndexList() {
    return Arrays.asList(0, 1);
  }

  public void setPredictIndexList(List<Integer> predictIndexList) {
    this.predictIndexList = predictIndexList;
  }

  public List<TSDataType> getInputTypeList() {
    return Arrays.asList(TSDataType.FLOAT, TSDataType.FLOAT);
  }

  public void setInputTypeList(List<TSDataType> inputTypeList) {
    this.inputTypeList = inputTypeList;
  }

  @Override
  public String getParametersString() {
    if (parametersString == null) {
      StringBuilder builder = new StringBuilder();
      builder.append("\"").append(MODEL_ID).append("\"=\"").append(modelId).append("\"");
      if (expectedPredictLength != modelPredictLength) {
        builder
            .append(", ")
            .append("\"")
            .append(PREDICT_LENGTH)
            .append("\"=\"")
            .append(expectedPredictLength)
            .append("\"");
      }
      parametersString = builder.toString();
    }
    return parametersString;
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
}
