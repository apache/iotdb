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

package org.apache.iotdb.db.queryengine.plan.expression.leaf;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.db.queryengine.execution.MemoryEstimationHelper;
import org.apache.iotdb.db.queryengine.plan.expression.ExpressionType;
import org.apache.iotdb.db.queryengine.plan.expression.visitor.ExpressionVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.queryengine.transformation.dag.memory.LayerMemoryAssigner;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class TimeSeriesOperand extends LeafOperand {

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(TimeSeriesOperand.class);

  private final PartialPath path;

  // if path is MeasurementPath or AlignedPath, this type is null
  private final TSDataType type;

  public TimeSeriesOperand(PartialPath path) {
    this.path = path;
    this.type = null;
  }

  public TimeSeriesOperand(PartialPath path, TSDataType dataType) {
    this.path = path;
    this.type = dataType;
  }

  public TimeSeriesOperand(ByteBuffer byteBuffer) {
    path = (PartialPath) PathDeserializeUtil.deserialize(byteBuffer);
    boolean hasType = ReadWriteIOUtils.readBool(byteBuffer);
    if (hasType) {
      this.type = TSDataType.deserializeFrom(byteBuffer);
    } else {
      this.type = null;
    }
  }

  public static TimeSeriesOperand constructColumnHeaderExpression(
      String columnName, TSDataType dataType) {
    return new TimeSeriesOperand(new PartialPath(columnName, false), dataType);
  }

  public PartialPath getPath() {
    return path;
  }

  // get TSDataType of this TimeSeriesOperand returning, it will never return null
  public TSDataType getOperandType() {
    return type != null ? type : path.getSeriesType();
  }

  // get the type field of this TimeSeriesOperand, it may return null
  public TSDataType getType() {
    return type;
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
  public void bindInputLayerColumnIndexWithExpression(
      Map<String, List<InputLocation>> inputLocations) {
    final String digest = getExpressionString();

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
    if (type == null) {
      ReadWriteIOUtils.write(false, byteBuffer);
    } else {
      ReadWriteIOUtils.write(true, byteBuffer);
      type.serializeTo(byteBuffer);
    }
  }

  @Override
  protected void serialize(DataOutputStream stream) throws IOException {
    path.serialize(stream);
    if (type == null) {
      ReadWriteIOUtils.write(false, stream);
    } else {
      ReadWriteIOUtils.write(true, stream);
      type.serializeTo(stream);
    }
  }

  @Override
  public long ramBytesUsed() {
    return INSTANCE_SIZE + MemoryEstimationHelper.getEstimatedSizeOfPartialPath(path);
  }
}
