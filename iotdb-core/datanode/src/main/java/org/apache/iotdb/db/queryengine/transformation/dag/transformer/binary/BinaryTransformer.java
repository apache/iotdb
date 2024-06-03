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

package org.apache.iotdb.db.queryengine.transformation.dag.transformer.binary;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.transformation.api.LayerReader;
import org.apache.iotdb.db.queryengine.transformation.api.YieldableState;
import org.apache.iotdb.db.queryengine.transformation.dag.transformer.Transformer;
import org.apache.iotdb.db.queryengine.transformation.dag.util.TypeUtils;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;

import java.io.IOException;

public abstract class BinaryTransformer extends Transformer {

  protected final LayerReader leftReader;
  protected final LayerReader rightReader;

  protected final TSDataType leftReaderDataType;
  protected final TSDataType rightReaderDataType;

  protected final boolean isLeftReaderConstant;
  protected final boolean isRightReaderConstant;

  protected Column[] leftColumns;
  protected Column[] rightColumns;

  protected int leftConsumed;
  protected int rightConsumed;

  protected final boolean isCurrentConstant;

  protected BinaryTransformer(LayerReader leftReader, LayerReader rightReader) {
    this.leftReader = leftReader;
    this.rightReader = rightReader;
    leftReaderDataType = leftReader.getDataTypes()[0];
    rightReaderDataType = rightReader.getDataTypes()[0];
    isLeftReaderConstant = leftReader.isConstantPointReader();
    isRightReaderConstant = rightReader.isConstantPointReader();
    isCurrentConstant = isLeftReaderConstant && isRightReaderConstant;
    checkType();
  }

  protected abstract void checkType();

  @Override
  public boolean isConstantPointReader() {
    return isCurrentConstant;
  }

  @Override
  public YieldableState yieldValue() throws Exception {
    // Generate data
    if (leftColumns == null) {
      YieldableState state = leftReader.yield();
      if (state != YieldableState.YIELDABLE) {
        return state;
      }
      leftColumns = leftReader.current();
    }
    if (rightColumns == null) {
      YieldableState state = rightReader.yield();
      if (state != YieldableState.YIELDABLE) {
        return state;
      }
      rightColumns = rightReader.current();
    }

    // Constant folding, or more precisely, constant caching
    if (isCurrentConstant && cachedColumns != null) {
      return YieldableState.YIELDABLE;
    }

    // Merge and transform
    int leftCount = leftColumns[0].getPositionCount();
    int rightCount = rightColumns[0].getPositionCount();
    int leftRemains = leftCount - leftConsumed;
    int rightRemains = rightCount - rightConsumed;
    int expectedEntries = Math.min(leftRemains, rightRemains);
    cachedColumns = mergeAndTransformColumns(expectedEntries);

    return YieldableState.YIELDABLE;
  }

  protected Column[] mergeAndTransformColumns(int count) throws QueryProcessException, IOException {
    TSDataType outputType = getDataTypes()[0];
    ColumnBuilder timeBuilder = new TimeColumnBuilder(null, count);
    ColumnBuilder valueBuilder = TypeUtils.initColumnBuilder(outputType, count);

    if (isLeftReaderConstant || isRightReaderConstant) {
      return handleConstantColumns(valueBuilder);
    }

    return handleNonConstantColumns(timeBuilder, valueBuilder);
  }

  private Column[] handleNonConstantColumns(ColumnBuilder timeBuilder, ColumnBuilder valueBuilder)
      throws QueryProcessException, IOException {
    Column leftTimes = leftColumns[1], leftValues = leftColumns[0];
    Column rightTimes = rightColumns[1], rightValues = rightColumns[0];

    int leftEnd = leftTimes.getPositionCount();
    int rightEnd = rightTimes.getPositionCount();

    // Combine two columns by merge sort
    while (leftConsumed < leftEnd && rightConsumed < rightEnd) {
      long leftTime = leftTimes.getLong(leftConsumed);
      long rightTime = rightTimes.getLong(rightConsumed);

      if (leftTime != rightTime) {
        if (leftTime < rightTime) {
          leftConsumed++;
        } else {
          rightConsumed++;
        }
      } else {
        timeBuilder.writeLong(leftTime);
        if (leftValues.isNull(leftConsumed) || rightValues.isNull(rightConsumed)) {
          valueBuilder.appendNull();
        } else {
          transformAndCache(leftValues, leftConsumed, rightValues, rightConsumed, valueBuilder);
        }

        leftConsumed++;
        rightConsumed++;
      }
    }

    // Clean up
    if (leftConsumed == leftEnd) {
      leftColumns = null;
      leftConsumed = 0;
      leftReader.consumedAll();
    }
    if (rightConsumed == rightEnd) {
      rightColumns = null;
      rightConsumed = 0;
      rightReader.consumedAll();
    }

    Column times = timeBuilder.build();
    Column values = valueBuilder.build();
    return new Column[] {values, times};
  }

  private Column[] handleConstantColumns(ColumnBuilder valueBuilder)
      throws QueryProcessException, IOException {
    if (isLeftReaderConstant && isRightReaderConstant) {
      transformAndCache(leftColumns[0], 0, rightColumns[0], 0, valueBuilder);
      Column constants = valueBuilder.build();
      return new Column[] {constants};
    } else if (isLeftReaderConstant) {
      for (int i = 0; i < rightColumns[0].getPositionCount(); i++) {
        if (rightColumns[0].isNull(i)) {
          valueBuilder.appendNull();
        } else {
          transformAndCache(leftColumns[0], 0, rightColumns[0], i, valueBuilder);
        }
      }
      Column times = rightColumns[1];
      Column values = valueBuilder.build();

      // Clean up
      rightColumns = null;
      rightReader.consumedAll();

      return new Column[] {values, times};
    } else if (isRightReaderConstant) {
      for (int i = 0; i < leftColumns[0].getPositionCount(); i++) {
        if (leftColumns[0].isNull(i)) {
          valueBuilder.appendNull();
        } else {
          transformAndCache(leftColumns[0], i, rightColumns[0], 0, valueBuilder);
        }
      }
      Column times = leftColumns[1];
      Column values = valueBuilder.build();

      // Clean up
      leftColumns = null;
      leftReader.consumedAll();

      return new Column[] {values, times};
    }

    // Unreachable
    return null;
  }

  protected abstract void transformAndCache(
      Column leftValues, int leftIndex, Column rightValues, int rightIndex, ColumnBuilder builder)
      throws QueryProcessException, IOException;
}
