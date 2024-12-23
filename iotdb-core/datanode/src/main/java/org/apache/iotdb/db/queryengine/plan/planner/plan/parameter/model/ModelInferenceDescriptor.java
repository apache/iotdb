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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.model.ModelInformation;
import org.apache.iotdb.db.queryengine.execution.operator.window.ainode.InferenceWindowParameter;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class ModelInferenceDescriptor {

  private final TEndPoint targetAINode;
  private final ModelInformation modelInformation;
  private List<String> outputColumnNames;
  private InferenceWindowParameter inferenceWindowParameter;
  private Map<String, String> inferenceAttributes;

  public ModelInferenceDescriptor(TEndPoint targetAINode, ModelInformation modelInformation) {
    this.targetAINode = targetAINode;
    this.modelInformation = modelInformation;
  }

  private ModelInferenceDescriptor(ByteBuffer buffer) {
    this.targetAINode =
        new TEndPoint(ReadWriteIOUtils.readString(buffer), ReadWriteIOUtils.readInt(buffer));
    this.modelInformation = ModelInformation.deserialize(buffer);
    int outputColumnNamesSize = ReadWriteIOUtils.readInt(buffer);
    if (outputColumnNamesSize == 0) {
      this.outputColumnNames = null;
    } else {
      this.outputColumnNames = new ArrayList<>();
      for (int i = 0; i < outputColumnNamesSize; i++) {
        this.outputColumnNames.add(ReadWriteIOUtils.readString(buffer));
      }
    }
    boolean hasInferenceWindowParameter = ReadWriteIOUtils.readBool(buffer);
    if (hasInferenceWindowParameter) {
      this.inferenceWindowParameter = InferenceWindowParameter.deserialize(buffer);
    } else {
      this.inferenceWindowParameter = null;
    }
    int inferenceAttributesSize = ReadWriteIOUtils.readInt(buffer);
    if (inferenceAttributesSize == 0) {
      this.inferenceAttributes = null;
    } else {
      this.inferenceAttributes = new HashMap<>();
      for (int i = 0; i < inferenceAttributesSize; i++) {
        this.inferenceAttributes.put(
            ReadWriteIOUtils.readString(buffer), ReadWriteIOUtils.readString(buffer));
      }
    }
  }

  public void setInferenceAttributes(Map<String, String> inferenceAttributes) {
    this.inferenceAttributes = inferenceAttributes;
  }

  public Map<String, String> getInferenceAttributes() {
    return inferenceAttributes;
  }

  public void setInferenceWindowParameter(InferenceWindowParameter inferenceWindowParameter) {
    this.inferenceWindowParameter = inferenceWindowParameter;
  }

  public InferenceWindowParameter getInferenceWindowParameter() {
    return inferenceWindowParameter;
  }

  public ModelInformation getModelInformation() {
    return modelInformation;
  }

  public TEndPoint getTargetAINode() {
    return targetAINode;
  }

  public String getModelName() {
    return modelInformation.getModelName();
  }

  public void setOutputColumnNames(List<String> outputColumnNames) {
    this.outputColumnNames = outputColumnNames;
  }

  public List<String> getOutputColumnNames() {
    return outputColumnNames;
  }

  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(targetAINode.ip, byteBuffer);
    ReadWriteIOUtils.write(targetAINode.port, byteBuffer);
    modelInformation.serialize(byteBuffer);
    if (outputColumnNames == null) {
      ReadWriteIOUtils.write(0, byteBuffer);
    } else {
      ReadWriteIOUtils.write(outputColumnNames.size(), byteBuffer);
      for (String outputColumnName : outputColumnNames) {
        ReadWriteIOUtils.write(outputColumnName, byteBuffer);
      }
    }
    if (inferenceWindowParameter == null) {
      ReadWriteIOUtils.write(false, byteBuffer);
    } else {
      ReadWriteIOUtils.write(true, byteBuffer);
      inferenceWindowParameter.serialize(byteBuffer);
    }
    if (inferenceAttributes == null) {
      ReadWriteIOUtils.write(0, byteBuffer);
    } else {
      ReadWriteIOUtils.write(inferenceAttributes.size(), byteBuffer);
      for (Map.Entry<String, String> entry : inferenceAttributes.entrySet()) {
        ReadWriteIOUtils.write(entry.getKey(), byteBuffer);
        ReadWriteIOUtils.write(entry.getValue(), byteBuffer);
      }
    }
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(targetAINode.ip, stream);
    ReadWriteIOUtils.write(targetAINode.port, stream);
    modelInformation.serialize(stream);
    if (outputColumnNames == null) {
      ReadWriteIOUtils.write(0, stream);
    } else {
      ReadWriteIOUtils.write(outputColumnNames.size(), stream);
      for (String outputColumnName : outputColumnNames) {
        ReadWriteIOUtils.write(outputColumnName, stream);
      }
    }
    if (inferenceWindowParameter == null) {
      ReadWriteIOUtils.write(false, stream);
    } else {
      ReadWriteIOUtils.write(true, stream);
      inferenceWindowParameter.serialize(stream);
    }
    if (inferenceAttributes == null) {
      ReadWriteIOUtils.write(0, stream);
    } else {
      ReadWriteIOUtils.write(inferenceAttributes.size(), stream);
      for (Map.Entry<String, String> entry : inferenceAttributes.entrySet()) {
        ReadWriteIOUtils.write(entry.getKey(), stream);
        ReadWriteIOUtils.write(entry.getValue(), stream);
      }
    }
  }

  public static ModelInferenceDescriptor deserialize(ByteBuffer buffer) {
    return new ModelInferenceDescriptor(buffer);
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
    return targetAINode.equals(that.targetAINode)
        && modelInformation.equals(that.modelInformation)
        && outputColumnNames.equals(that.outputColumnNames)
        && inferenceWindowParameter.equals(that.inferenceWindowParameter)
        && inferenceAttributes.equals(that.inferenceAttributes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        targetAINode,
        modelInformation,
        outputColumnNames,
        inferenceWindowParameter,
        inferenceAttributes);
  }
}
