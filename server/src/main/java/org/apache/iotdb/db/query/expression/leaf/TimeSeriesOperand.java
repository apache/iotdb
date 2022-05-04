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

package org.apache.iotdb.db.query.expression.leaf;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.query.LogicalOptimizeException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
import org.apache.iotdb.db.metadata.path.PathDeserializeUtil;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.db.mpp.plan.rewriter.WildcardsRemover;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.db.query.expression.Expression;
import org.apache.iotdb.db.query.expression.ExpressionType;
import org.apache.iotdb.db.query.udf.core.executor.UDTFContext;
import org.apache.iotdb.db.query.udf.core.layer.IntermediateLayer;
import org.apache.iotdb.db.query.udf.core.layer.LayerMemoryAssigner;
import org.apache.iotdb.db.query.udf.core.layer.RawQueryInputLayer;
import org.apache.iotdb.db.query.udf.core.layer.SingleInputColumnMultiReferenceIntermediateLayer;
import org.apache.iotdb.db.query.udf.core.layer.SingleInputColumnSingleReferenceIntermediateLayer;
import org.apache.iotdb.db.query.udf.core.reader.LayerPointReader;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TimeSeriesOperand extends LeafOperand {

  private PartialPath path;

  public TimeSeriesOperand(PartialPath path) {
    this.path = path;
  }

  public TimeSeriesOperand(ByteBuffer byteBuffer) {
    path = (PartialPath) PathDeserializeUtil.deserialize(byteBuffer);
  }

  public PartialPath getPath() {
    return path;
  }

  public void setPath(PartialPath path) {
    this.path = path;
  }

  @Override
  public boolean isConstantOperandInternal() {
    return false;
  }

  @Override
  public void concat(
      List<PartialPath> prefixPaths,
      List<Expression> resultExpressions,
      PathPatternTree patternTree) {
    for (PartialPath prefixPath : prefixPaths) {
      TimeSeriesOperand resultExpression = new TimeSeriesOperand(prefixPath.concatPath(path));
      patternTree.appendPath(resultExpression.getPath());
      resultExpressions.add(resultExpression);
    }
  }

  @Override
  public void concat(List<PartialPath> prefixPaths, List<Expression> resultExpressions) {
    for (PartialPath prefixPath : prefixPaths) {
      resultExpressions.add(new TimeSeriesOperand(prefixPath.concatPath(path)));
    }
  }

  @Override
  public void removeWildcards(WildcardsRemover wildcardsRemover, List<Expression> resultExpressions)
      throws StatementAnalyzeException {
    for (PartialPath actualPath : wildcardsRemover.removeWildcardInPath(path)) {
      resultExpressions.add(new TimeSeriesOperand(actualPath));
    }
  }

  @Override
  public void removeWildcards(
      org.apache.iotdb.db.qp.utils.WildcardsRemover wildcardsRemover,
      List<Expression> resultExpressions)
      throws LogicalOptimizeException {
    for (PartialPath actualPath : wildcardsRemover.removeWildcardFrom(path)) {
      resultExpressions.add(new TimeSeriesOperand(actualPath));
    }
  }

  @Override
  public void collectPaths(Set<PartialPath> pathSet) {
    pathSet.add(path);
  }

  @Override
  public void bindInputLayerColumnIndexWithExpression(UDTFPlan udtfPlan) {
    inputColumnIndex = udtfPlan.getReaderIndexByExpressionName(toString());
  }

  @Override
  public void updateStatisticsForMemoryAssigner(LayerMemoryAssigner memoryAssigner) {
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
      throws QueryProcessException {
    if (!expressionIntermediateLayerMap.containsKey(this)) {
      float memoryBudgetInMB = memoryAssigner.assign();

      LayerPointReader parentLayerPointReader =
          rawTimeSeriesInputLayer.constructValuePointReader(inputColumnIndex);
      expressionDataTypeMap.put(this, parentLayerPointReader.getDataType());

      expressionIntermediateLayerMap.put(
          this,
          memoryAssigner.getReference(this) == 1
              ? new SingleInputColumnSingleReferenceIntermediateLayer(
                  this, queryId, memoryBudgetInMB, parentLayerPointReader)
              : new SingleInputColumnMultiReferenceIntermediateLayer(
                  this, queryId, memoryBudgetInMB, parentLayerPointReader));
    }

    return expressionIntermediateLayerMap.get(this);
  }

  public String getExpressionStringInternal() {
    return path.isMeasurementAliasExists() ? path.getFullPathWithAlias() : path.getFullPath();
  }

  @Override
  public ExpressionType getExpressionType() {
    return ExpressionType.TIMESERIES;
  }

  @Override
  protected void serialize(ByteBuffer byteBuffer) {
    path.serialize(byteBuffer);
  }
}
