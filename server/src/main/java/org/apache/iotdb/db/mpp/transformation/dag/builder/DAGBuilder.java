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

package org.apache.iotdb.db.mpp.transformation.dag.builder;

import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.ResultColumn;
import org.apache.iotdb.db.mpp.plan.expression.visitor.OldIntermediateLayerVisitor;
import org.apache.iotdb.db.mpp.transformation.api.LayerPointReader;
import org.apache.iotdb.db.mpp.transformation.dag.input.QueryDataSetInputLayer;
import org.apache.iotdb.db.mpp.transformation.dag.intermediate.IntermediateLayer;
import org.apache.iotdb.db.mpp.transformation.dag.memory.LayerMemoryAssigner;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.HashMap;
import java.util.Map;

public class DAGBuilder {

  private final long queryId;
  private final UDTFPlan udtfPlan;
  private final QueryDataSetInputLayer rawTimeSeriesInputLayer;

  // input
  private final Expression[] resultColumnExpressions;
  // output
  private final LayerPointReader[] resultColumnPointReaders;

  private final LayerMemoryAssigner memoryAssigner;

  // all result column expressions will be split into several sub-expressions, each expression has
  // its own result point reader. different result column expressions may have the same
  // sub-expressions, but they can share the same point reader. we cache the point reader here to
  // make sure that only one point reader will be built for one expression.
  private final Map<Expression, IntermediateLayer> expressionIntermediateLayerMap;
  private final Map<Expression, TSDataType> expressionDataTypeMap;

  public DAGBuilder(
      long queryId, UDTFPlan udtfPlan, QueryDataSetInputLayer inputLayer, float memoryBudgetInMB) {
    this.queryId = queryId;
    this.udtfPlan = udtfPlan;
    this.rawTimeSeriesInputLayer = inputLayer;

    int size = udtfPlan.getPathToIndex().size();
    resultColumnExpressions = new Expression[size];
    for (int i = 0; i < size; ++i) {
      resultColumnExpressions[i] = udtfPlan.getResultColumnByDatasetOutputIndex(i).getExpression();
    }
    resultColumnPointReaders = new LayerPointReader[size];

    memoryAssigner = new LayerMemoryAssigner(memoryBudgetInMB);

    expressionIntermediateLayerMap = new HashMap<>();
    expressionDataTypeMap = new HashMap<>();
  }

  public DAGBuilder bindInputLayerColumnIndexWithExpression() {
    for (Expression expression : resultColumnExpressions) {
      expression.bindInputLayerColumnIndexWithExpression(udtfPlan);
    }
    return this;
  }

  public DAGBuilder buildLayerMemoryAssigner() {
    for (Expression expression : resultColumnExpressions) {
      expression.updateStatisticsForMemoryAssigner(memoryAssigner);
    }
    memoryAssigner.build();
    return this;
  }

  public DAGBuilder buildResultColumnPointReaders() {
    OldIntermediateLayerVisitor visitor = new OldIntermediateLayerVisitor();
    OldIntermediateLayerVisitor.OldIntermediateLayerVisitorContext context =
        new OldIntermediateLayerVisitor.OldIntermediateLayerVisitorContext(
            queryId,
            udtfPlan.getUdtfContext(),
            rawTimeSeriesInputLayer,
            expressionIntermediateLayerMap,
            expressionDataTypeMap,
            memoryAssigner);
    for (int i = 0; i < resultColumnExpressions.length; ++i) {
      resultColumnPointReaders[i] =
          visitor.process(resultColumnExpressions[i], context).constructPointReader();
    }
    return this;
  }

  public DAGBuilder setDataSetResultColumnDataTypes() {
    for (ResultColumn resultColumn : udtfPlan.getResultColumns()) {
      resultColumn.setDataType(expressionDataTypeMap.get(resultColumn.getExpression()));
    }
    return this;
  }

  public LayerPointReader[] getResultColumnPointReaders() {
    return resultColumnPointReaders;
  }
}
