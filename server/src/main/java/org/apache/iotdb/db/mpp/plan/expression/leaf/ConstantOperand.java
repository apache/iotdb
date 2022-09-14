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
import org.apache.iotdb.db.mpp.plan.expression.Expression;
import org.apache.iotdb.db.mpp.plan.expression.ExpressionType;
import org.apache.iotdb.db.mpp.plan.expression.visitor.ExpressionVisitor;
import org.apache.iotdb.db.mpp.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.mpp.transformation.dag.memory.LayerMemoryAssigner;
import org.apache.iotdb.db.qp.physical.crud.UDTFPlan;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import org.apache.commons.lang3.Validate;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Constant operand */
public class ConstantOperand extends LeafOperand {

  private final String valueString;
  private final TSDataType dataType;

  public ConstantOperand(TSDataType dataType, String valueString) {
    this.dataType = Validate.notNull(dataType);
    this.valueString = Validate.notNull(valueString);
  }

  public ConstantOperand(ByteBuffer byteBuffer) {
    dataType = TSDataType.deserializeFrom(byteBuffer);
    valueString = ReadWriteIOUtils.readString(byteBuffer);
  }

  @Override
  public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
    return visitor.visitConstantOperand(this, context);
  }

  public TSDataType getDataType() {
    return dataType;
  }

  public String getValueString() {
    return valueString;
  }

  public boolean isNegativeNumber() {
    return !dataType.equals(TSDataType.TEXT)
        && !dataType.equals(TSDataType.BOOLEAN)
        && Double.parseDouble(valueString) < 0;
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
      org.apache.iotdb.db.qp.utils.WildcardsRemover wildcardsRemover,
      List<Expression> resultExpressions) {
    resultExpressions.add(this);
  }

  @Override
  public void collectPaths(Set<PartialPath> pathSet) {
    // Do nothing
  }

  @Override
  public void bindInputLayerColumnIndexWithExpression(UDTFPlan udtfPlan) {
    // Do nothing
  }

  @Override
  public void bindInputLayerColumnIndexWithExpression(
      Map<String, List<InputLocation>> inputLocations) {
    // Do nothing
  }

  @Override
  public void updateStatisticsForMemoryAssigner(LayerMemoryAssigner memoryAssigner) {
    // Do nothing
  }

  @Override
  public String getExpressionStringInternal() {
    return valueString;
  }

  @Override
  public ExpressionType getExpressionType() {
    return ExpressionType.CONSTANT;
  }

  @Override
  protected void serialize(ByteBuffer byteBuffer) {
    dataType.serializeTo(byteBuffer);
    ReadWriteIOUtils.write(valueString, byteBuffer);
  }

  @Override
  protected void serialize(DataOutputStream stream) throws IOException {
    dataType.serializeTo(stream);
    ReadWriteIOUtils.write(valueString, stream);
  }
}
