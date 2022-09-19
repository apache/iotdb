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

package org.apache.iotdb.db.mpp.plan.expression.leaf;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.query.LogicalOptimizeException;
import org.apache.iotdb.db.metadata.path.PathDeserializeUtil;
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.ExpressionType;
import org.apache.iotdb.db.mpp.plan.expression.visitor.ExpressionVisitor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.mpp.transformation.dag.memory.LayerMemoryAssigner;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;

import java.io.DataOutputStream;
import java.io.IOException;
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
  public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitTimeSeriesOperand(this, context);
  }

  @Override
  public boolean isConstantOperandInternal() {
    return false;
  }

  @Override
  public void concat(List<PartialPath> prefixPaths, List<Expression> resultExpressions) {
    for (PartialPath prefixPath : prefixPaths) {
      resultExpressions.add(new TimeSeriesOperand(prefixPath.concatPath(path)));
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
  public void bindInputLayerColumnIndexWithExpression(
      Map<String, List<InputLocation>> inputLocations) {
    final String digest = toString();

    if (inputLocations.containsKey(digest)) {
      inputColumnIndex = inputLocations.get(digest).get(0).getValueColumnIndex();
    }
  }

  @Override
  public void updateStatisticsForMemoryAssigner(LayerMemoryAssigner memoryAssigner) {
    memoryAssigner.increaseExpressionReference(this);
  }

  @Override
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

  @Override
  protected void serialize(DataOutputStream stream) throws IOException {
    path.serialize(stream);
  }
}
