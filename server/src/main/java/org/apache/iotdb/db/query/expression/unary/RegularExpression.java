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

import org.apache.iotdb.db.exception.query.LogicalOptimizeException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.db.mpp.sql.rewriter.WildcardsRemover;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.ExpressionType;
import org.apache.iotdb.db.query.udf.core.executor.UDTFContext;
import org.apache.iotdb.db.query.udf.core.executor.UDTFExecutor;
import org.apache.iotdb.db.query.udf.core.layer.IntermediateLayer;
import org.apache.iotdb.db.query.udf.core.layer.LayerMemoryAssigner;
import org.apache.iotdb.db.query.udf.core.layer.RawQueryInputLayer;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.commons.lang3.Validate;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class RegularExpression extends Expression {

  private final Expression expression;
  private final String patternString;
  private final Pattern pattern;

  public RegularExpression(Expression expression, String patternString) {
    this.expression = expression;
    this.patternString = patternString;
    pattern = Pattern.compile(patternString);
  }

  public RegularExpression(Expression expression, String patternString, Pattern pattern) {
    this.expression = expression;
    this.patternString = patternString;
    this.pattern = pattern;
  }

  public RegularExpression(ByteBuffer byteBuffer) {
    expression = Expression.deserialize(byteBuffer);
    patternString = ReadWriteIOUtils.readString(byteBuffer);
    pattern = Pattern.compile(Validate.notNull(patternString));
  }

  @Override
  public void concat(
      List<PartialPath> prefixPaths,
      List<Expression> resultExpressions,
      PathPatternTree patternTree) {
    List<Expression> resultExpressionsForRecursion = new ArrayList<>();
    expression.concat(prefixPaths, resultExpressionsForRecursion, patternTree);
    for (Expression resultExpression : resultExpressionsForRecursion) {
      resultExpressions.add(new RegularExpression(resultExpression, patternString, pattern));
    }
  }

  @Override
  public void concat(List<PartialPath> prefixPaths, List<Expression> resultExpressions) {
    List<Expression> resultExpressionsForRecursion = new ArrayList<>();
    expression.concat(prefixPaths, resultExpressionsForRecursion);
    for (Expression resultExpression : resultExpressionsForRecursion) {
      resultExpressions.add(new RegularExpression(resultExpression, patternString, pattern));
    }
  }

  @Override
  public void removeWildcards(WildcardsRemover wildcardsRemover, List<Expression> resultExpressions)
      throws StatementAnalyzeException {
    List<Expression> resultExpressionsForRecursion = new ArrayList<>();
    expression.removeWildcards(wildcardsRemover, resultExpressionsForRecursion);
    for (Expression resultExpression : resultExpressionsForRecursion) {
      resultExpressions.add(new RegularExpression(resultExpression, patternString, pattern));
    }
  }

  @Override
  public void removeWildcards(
      org.apache.iotdb.db.qp.utils.WildcardsRemover wildcardsRemover,
      List<Expression> resultExpressions)
      throws LogicalOptimizeException {
    List<Expression> resultExpressionsForRecursion = new ArrayList<>();
    expression.removeWildcards(wildcardsRemover, resultExpressionsForRecursion);
    for (Expression resultExpression : resultExpressionsForRecursion) {
      resultExpressions.add(new RegularExpression(resultExpression, patternString, pattern));
    }
  }

  @Override
  public void collectPaths(Set<PartialPath> pathSet) {
    expression.collectPaths(pathSet);
  }

  @Override
  public void constructUdfExecutors(
      Map<String, UDTFExecutor> expressionName2Executor, ZoneId zoneId) {
    expression.constructUdfExecutors(expressionName2Executor, zoneId);
  }

  @Override
  public void bindInputLayerColumnIndexWithExpression(UDTFPlan udtfPlan) {
    expression.bindInputLayerColumnIndexWithExpression(udtfPlan);
    inputColumnIndex = udtfPlan.getReaderIndexByExpressionName(toString());
  }

  @Override
  public void updateStatisticsForMemoryAssigner(LayerMemoryAssigner memoryAssigner) {
    expression.updateStatisticsForMemoryAssigner(memoryAssigner);
    memoryAssigner.increaseExpressionReference(this);
  }

  @Override
  public IntermediateLayer constructIntermediateLayer(
      long queryId,
      UDTFContext udtfContext,
      RawQueryInputLayer rawTimeSeriesInputLayer,
      Map<Expression, IntermediateLayer> expressionIntermediateLayerMap,
      Map<Expression, TSDataType> expressionDataTypeMap,
      LayerMemoryAssigner memoryAssigner)
      throws QueryProcessException, IOException {
    // TODO
    throw new RuntimeException();
  }

  @Override
  protected boolean isConstantOperandInternal() {
    return expression.isConstantOperand();
  }

  @Override
  public List<Expression> getExpressions() {
    return Collections.singletonList(expression);
  }

  @Override
  protected String getExpressionStringInternal() {
    // TODO
    throw new RuntimeException();
  }

  @Override
  protected short getExpressionType() {
    return ExpressionType.REGULAR.getExpressionType();
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    Expression.serialize(expression, byteBuffer);
    ReadWriteIOUtils.write(patternString, byteBuffer);
  }

  @Override
  public boolean isTimeSeriesGeneratingFunctionExpression() {
    return !isUserDefinedAggregationFunctionExpression();
  }

  @Override
  public boolean isUserDefinedAggregationFunctionExpression() {
    return expression.isUserDefinedAggregationFunctionExpression()
        || expression.isBuiltInAggregationFunctionExpression();
  }
}
