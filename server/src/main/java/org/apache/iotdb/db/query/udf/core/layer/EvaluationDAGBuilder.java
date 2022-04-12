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

package org.apache.iotdb.db.query.udf.core.layer;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.udf.core.executor.UDTFContext;
import org.apache.iotdb.db.query.udf.core.reader.LayerPointReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class EvaluationDAGBuilder {

  private final long queryId;
  private final RawQueryInputLayer inputLayer;

  private final Expression[] resultColumnExpressions;
  private final LayerPointReader[] resultColumnPointReaders;

  private final UDTFContext udtfContext;

  private final LayerMemoryAssigner memoryAssigner;

  // all result column expressions will be split into several sub-expressions, each expression has
  // its own result point reader. different result column expressions may have the same
  // sub-expressions, but they can share the same point reader. we cache the point reader here to
  // make sure that only one point reader will be built for one expression.
  private final Map<Expression, IntermediateLayer> expressionIntermediateLayerMap;
  private final Map<Expression, TSDataType> expressionDataTypeMap;

  public EvaluationDAGBuilder(
      long queryId,
      Expression[] resultColumnExpressions,
      RawQueryInputLayer inputLayer,
      UDTFContext udtfContext,
      float memoryBudgetInMB) {
    this.queryId = queryId;
    this.resultColumnExpressions = resultColumnExpressions;
    this.inputLayer = inputLayer;
    this.udtfContext = udtfContext;

    int size = inputLayer.getInputColumnCount();
    resultColumnPointReaders = new LayerPointReader[size];

    memoryAssigner = new LayerMemoryAssigner(memoryBudgetInMB);

    expressionIntermediateLayerMap = new HashMap<>();
    expressionDataTypeMap = new HashMap<>();
  }

  public EvaluationDAGBuilder buildLayerMemoryAssigner() {
    for (Expression expression : resultColumnExpressions) {
      expression.updateStatisticsForMemoryAssigner(memoryAssigner);
    }
    memoryAssigner.build();
    return this;
  }

  public EvaluationDAGBuilder buildResultColumnPointReaders()
      throws QueryProcessException, IOException {
    for (int i = 0; i < resultColumnExpressions.length; ++i) {
      resultColumnPointReaders[i] =
          resultColumnExpressions[i]
              .constructIntermediateLayer(
                  queryId,
                  udtfContext,
                  inputLayer,
                  expressionIntermediateLayerMap,
                  expressionDataTypeMap,
                  memoryAssigner)
              .constructPointReader();
    }
    return this;
  }

  public LayerPointReader[] getResultColumnPointReaders() {
    return resultColumnPointReaders;
  }
}
