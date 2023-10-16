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

package org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.model;

import org.apache.iotdb.commons.model.ModelInformation;
import org.apache.iotdb.commons.udf.builtin.ModelInferenceFunction;
import org.apache.iotdb.db.queryengine.plan.expression.multi.FunctionExpression;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;

public abstract class ModelInferenceDescriptor {

  protected final ModelInferenceFunction functionType;

  protected final String modelId;

  protected final String modelPath;

  protected List<FunctionExpression> modelInferenceOutputExpressions;

  public ModelInferenceDescriptor(
      ModelInferenceFunction functionType, ModelInformation modelInformation) {
    this.functionType = functionType;
    this.modelId = modelInformation.getModelId();
    this.modelPath = modelInformation.getModelPath();
  }

  public ModelInferenceDescriptor(ByteBuffer buffer) {
    this.functionType = ModelInferenceFunction.values()[ReadWriteIOUtils.readInt(buffer)];
    this.modelId = ReadWriteIOUtils.readString(buffer);
    this.modelPath = ReadWriteIOUtils.readString(buffer);
  }

  public ModelInferenceFunction getFunctionType() {
    return functionType;
  }

  public String getModelId() {
    return modelId;
  }

  public String getModelPath() {
    return modelPath;
  }

  public List<FunctionExpression> getModelInferenceOutputExpressions() {
    return modelInferenceOutputExpressions;
  }

  public void setModelInferenceOutputExpressions(
      List<FunctionExpression> modelInferenceOutputExpressions) {
    this.modelInferenceOutputExpressions = modelInferenceOutputExpressions;
  }

  public abstract LinkedHashMap<String, String> getOutputAttributes();

  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(functionType.ordinal(), byteBuffer);
    ReadWriteIOUtils.write(modelId, byteBuffer);
    ReadWriteIOUtils.write(modelPath, byteBuffer);
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(functionType.ordinal(), stream);
    ReadWriteIOUtils.write(modelId, stream);
    ReadWriteIOUtils.write(modelPath, stream);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ModelInferenceDescriptor that = (ModelInferenceDescriptor) o;
    return functionType == that.functionType
        && modelId.equals(that.modelId)
        && modelPath.equals(that.modelPath)
        && modelInferenceOutputExpressions.equals(that.modelInferenceOutputExpressions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(functionType, modelId, modelPath, modelInferenceOutputExpressions);
  }
}
