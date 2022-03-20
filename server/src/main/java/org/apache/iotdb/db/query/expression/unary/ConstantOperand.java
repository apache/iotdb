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

package org.apache.iotdb.db.query.expression.unary;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.udf.core.executor.UDTFExecutor;
import org.apache.iotdb.db.query.udf.core.layer.ConstantIntermediateLayer;
import org.apache.iotdb.db.query.udf.core.layer.IntermediateLayer;
import org.apache.iotdb.db.query.udf.core.layer.LayerMemoryAssigner;
import org.apache.iotdb.db.query.udf.core.layer.RawQueryInputLayer;
import org.apache.iotdb.db.sql.rewriter.WildcardsRemover;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.apache.commons.lang3.Validate;

import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Constant operand */
public class ConstantOperand extends Expression {

  private final String valueString;
  private final TSDataType dataType;

  public ConstantOperand(TSDataType dataType, String str) throws QueryProcessException {
    this.dataType = Validate.notNull(dataType);
    this.valueString = Validate.notNull(str);
  }

  public TSDataType getDataType() {
    return dataType;
  }

  @Override
  public boolean isConstantOperandInternal() {
    return true;
  }

  @Override
  public void concat(List<PartialPath> prefixPaths, List<Expression> resultExpressions) {
    resultExpressions.add(this);
  }

  @Override
  public void removeWildcards(
      WildcardsRemover wildcardsRemover, List<Expression> resultExpressions) {
    resultExpressions.add(this);
  }

  @Override
  public void removeWildcards(
      org.apache.iotdb.db.qp.utils.WildcardsRemover wildcardsRemover,
      List<Expression> resultExpressions) {
    resultExpressions.add(this);
  }

  @Override
  public void collectPaths(Set<PartialPath> pathSet) {
    // Do nothing
  }

  @Override
  public List<Expression> getExpressions() {
    return Collections.emptyList();
  }

  @Override
  public void constructUdfExecutors(
      Map<String, UDTFExecutor> expressionName2Executor, ZoneId zoneId) {
    // Do nothing
  }

  @Override
  public void updateStatisticsForMemoryAssigner(LayerMemoryAssigner memoryAssigner) {
    // Do nothing
  }

  @Override
  public IntermediateLayer constructIntermediateLayer(
      long queryId,
      UDTFPlan udtfPlan,
      RawQueryInputLayer rawTimeSeriesInputLayer,
      Map<Expression, IntermediateLayer> expressionIntermediateLayerMap,
      Map<Expression, TSDataType> expressionDataTypeMap,
      LayerMemoryAssigner memoryAssigner)
      throws QueryProcessException {
    if (!expressionIntermediateLayerMap.containsKey(this)) {
      expressionDataTypeMap.put(this, this.getDataType());
      IntermediateLayer intermediateLayer =
          new ConstantIntermediateLayer(this, queryId, memoryAssigner.assign());
      expressionIntermediateLayerMap.put(this, intermediateLayer);
    }

    return expressionIntermediateLayerMap.get(this);
  }

  @Override
  public String getExpressionStringInternal() {
    return valueString;
  }
}
