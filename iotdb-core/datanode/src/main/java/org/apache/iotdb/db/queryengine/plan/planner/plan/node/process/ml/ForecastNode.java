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

package org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.ml;

import org.apache.iotdb.db.queryengine.plan.expression.Expression;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.process.SingleChildProcessNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.model.ForecastModelInferenceDescriptor;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ForecastNode extends SingleChildProcessNode {

  private final ForecastModelInferenceDescriptor modelInferenceDescriptor;

  private List<String> outputColumnNames;

  public ForecastNode(
      PlanNodeId id, PlanNode child, ForecastModelInferenceDescriptor modelInferenceDescriptor) {
    super(id, child);
    this.modelInferenceDescriptor = modelInferenceDescriptor;
  }

  public ForecastNode(PlanNodeId id, ForecastModelInferenceDescriptor modelInferenceDescriptor) {
    super(id);
    this.modelInferenceDescriptor = modelInferenceDescriptor;
  }

  public ForecastModelInferenceDescriptor getModelInferenceDescriptor() {
    return modelInferenceDescriptor;
  }

  @Override
  public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
    return visitor.visitForecast(this, context);
  }

  @Override
  public PlanNode clone() {
    return new ForecastNode(getPlanNodeId(), child, modelInferenceDescriptor);
  }

  @Override
  public List<String> getOutputColumnNames() {
    if (outputColumnNames == null) {
      outputColumnNames = new ArrayList<>();
      for (Expression expression : modelInferenceDescriptor.getModelInferenceOutputExpressions()) {
        outputColumnNames.add(expression.toString());
      }
    }
    return outputColumnNames;
  }

  @Override
  protected void serializeAttributes(ByteBuffer byteBuffer) {
    PlanNodeType.FORECAST.serialize(byteBuffer);
    modelInferenceDescriptor.serialize(byteBuffer);
  }

  @Override
  protected void serializeAttributes(DataOutputStream stream) throws IOException {
    PlanNodeType.FORECAST.serialize(stream);
    modelInferenceDescriptor.serialize(stream);
  }

  public static ForecastNode deserialize(ByteBuffer buffer) {
    ForecastModelInferenceDescriptor modelInferenceDescriptor =
        ForecastModelInferenceDescriptor.deserialize(buffer);
    PlanNodeId planNodeId = PlanNodeId.deserialize(buffer);
    return new ForecastNode(planNodeId, modelInferenceDescriptor);
  }

  @Override
  public String toString() {
    return "ForecastNode-" + this.getPlanNodeId();
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
    ForecastNode that = (ForecastNode) o;
    return modelInferenceDescriptor.equals(that.modelInferenceDescriptor);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), modelInferenceDescriptor);
  }
}
