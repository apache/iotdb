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

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.plan.analyze.TypeProvider;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.mpp.transformation.api.LayerPointReader;
import org.apache.iotdb.db.mpp.transformation.dag.input.QueryDataSetInputLayer;
import org.apache.iotdb.db.mpp.transformation.dag.intermediate.IntermediateLayer;
import org.apache.iotdb.db.mpp.transformation.dag.memory.LayerMemoryAssigner;
import org.apache.iotdb.db.mpp.transformation.dag.udf.UDTFContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EvaluationDAGBuilder {

  private final long queryId;

  private final QueryDataSetInputLayer inputLayer;
  private final Map<String, List<InputLocation>> inputLocations;

  private final Expression[] outputExpressions;
  private final LayerPointReader[] outputPointReaders;

  private final TypeProvider typeProvider;

  private final UDTFContext udtfContext;

  private final LayerMemoryAssigner memoryAssigner;

  // all result column expressions will be split into several sub-expressions, each expression has
  // its own result point reader. different result column expressions may have the same
  // sub-expressions, but they can share the same point reader. we cache the point reader here to
  // make sure that only one point reader will be built for one expression.
  private final Map<Expression, IntermediateLayer> expressionIntermediateLayerMap;

  public EvaluationDAGBuilder(
      long queryId,
      QueryDataSetInputLayer inputLayer,
      Map<String, List<InputLocation>> inputLocations,
      Expression[] outputExpressions,
      TypeProvider typeProvider,
      UDTFContext udtfContext,
      float memoryBudgetInMB) {
    this.queryId = queryId;
    this.inputLayer = inputLayer;
    this.inputLocations = inputLocations;
    this.outputExpressions = outputExpressions;
    this.typeProvider = typeProvider;
    this.udtfContext = udtfContext;

    outputPointReaders = new LayerPointReader[outputExpressions.length];

    memoryAssigner = new LayerMemoryAssigner(memoryBudgetInMB);

    expressionIntermediateLayerMap = new HashMap<>();
  }

  public EvaluationDAGBuilder buildLayerMemoryAssigner() {
    for (Expression expression : outputExpressions) {
      expression.updateStatisticsForMemoryAssigner(memoryAssigner);
    }
    memoryAssigner.build();
    return this;
  }

  public EvaluationDAGBuilder bindInputLayerColumnIndexWithExpression() {
    for (Expression expression : outputExpressions) {
      expression.bindInputLayerColumnIndexWithExpression(inputLocations);
    }
    return this;
  }

  public EvaluationDAGBuilder buildResultColumnPointReaders()
      throws QueryProcessException, IOException {
    for (int i = 0; i < outputExpressions.length; ++i) {
      outputPointReaders[i] =
          outputExpressions[i]
              .constructIntermediateLayer(
                  queryId,
                  udtfContext,
                  inputLayer,
                  expressionIntermediateLayerMap,
                  typeProvider,
                  memoryAssigner)
              .constructPointReader();
    }
    return this;
  }

  public LayerPointReader[] getOutputPointReaders() {
    return outputPointReaders;
  }
}
