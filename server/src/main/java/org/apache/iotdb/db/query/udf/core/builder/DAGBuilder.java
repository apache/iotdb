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

package org.apache.iotdb.db.query.udf.core.builder;

import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.ResultColumn;
import org.apache.iotdb.db.query.udf.core.transformer.Transformer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DAGBuilder {

  private final UDTFPlan udtfPlan;

  // input
  private final List<Expression> resultColumnExpressions;
  // output
  private final Transformer[] resultColumnTransformers;

  // all result column expressions will be split into several sub-expressions, each expression has
  // its own transformer. different result column expressions may have the same sub-expressions,
  // but they can share the same transformer. we cache the transformer builder here to make sure
  // that only one transformer will be built for one expression.
  private final Map<Expression, TransformerBuilder> expressionTransformerBuilderMap;

  public DAGBuilder(UDTFPlan udtfPlan) {
    this.udtfPlan = udtfPlan;
    resultColumnExpressions = new ArrayList<>();
    for (ResultColumn resultColumn : udtfPlan.getResultColumns()) {
      resultColumnExpressions.add(resultColumn.getExpression());
    }
    resultColumnTransformers = new Transformer[resultColumnExpressions.size()];
    expressionTransformerBuilderMap = new HashMap<>();

    build();
  }

  public void build() {
    constructTransformerBuilder();
    buildTransformer();
    buildDAG();
  }

  private void constructTransformerBuilder() {
    for (Expression resultColumnExpression : resultColumnExpressions) {
      resultColumnExpression.constructTransformerBuilder(expressionTransformerBuilderMap);
    }
  }

  private void buildTransformer() {}

  private void buildDAG() {}

  public Transformer[] getResultColumnTransformers() {
    return resultColumnTransformers;
  }
}
