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
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;

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

    int leftCount = leftColumns[0].getPositionCount() - leftConsumed;
    int rightCount = rightColumns[0].getPositionCount() - rightConsumed;
    if (leftCount < rightCount) {
      // Consume all left columns
      cachedColumns = mergeAndTransformColumns(leftCount);
      // No need to call rightReader.consume()

      // Clean up
      leftColumns = null;
      leftConsumed = 0;
      leftReader.consumedAll();
    } else {
      // Consume all right columns
      cachedColumns = mergeAndTransformColumns(rightCount);
      // No need to call rightReader.consume()

      // Clean up right columns
      rightColumns = null;
      rightConsumed = 0;
      rightReader.consumedAll();
    }

    return YieldableState.YIELDABLE;
  }

  protected Column[] mergeAndTransformColumns(int count) throws QueryProcessException, IOException {
    // TODO: maybe we should choose more precise expectedEntries
    TSDataType outputType = getDataTypes()[0];
    ColumnBuilder timeBuilder = new TimeColumnBuilder(null, count);
    ColumnBuilder valueBuilder = TypeUtils.initColumnBuilder(outputType, count);

    if (isLeftReaderConstant || isRightReaderConstant) {
      int leftOffset = leftConsumed;
      int rightOffset = rightConsumed;
      for (int i = 0; i < count; i++) {
        if (leftColumns[0].isNull(leftConsumed) || rightColumns[0].isNull(rightConsumed)) {
          valueBuilder.appendNull();
        } else {
          transformAndCache(
              leftColumns[0], leftConsumed, rightColumns[0], rightConsumed, valueBuilder);
        }

        leftConsumed++;
        rightConsumed++;
      }

      Column values = valueBuilder.build();
      if (isCurrentConstant) {
        return new Column[] {values};
      }
      Column times;
      if (isLeftReaderConstant) {
        times = rightColumns[1].getRegion(rightOffset, count);
      } else {
        times = leftColumns[1].getRegion(leftOffset, count);
      }
      return new Column[] {values, times};
    }

    Column leftTimes = leftColumns[1], leftValues = leftColumns[0];
    Column rightTimes = rightColumns[1], rightValues = rightColumns[0];

    int leftEnd = leftTimes.getPositionCount();
    int rightEnd = rightTimes.getPositionCount();

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
      }
    }

    Column times = timeBuilder.build();
    Column values = valueBuilder.build();
    return new Column[] {values, times};
  }

  protected abstract void transformAndCache(
      Column leftValues, int leftIndex, Column rightValues, int rightIndex, ColumnBuilder builder)
      throws QueryProcessException, IOException;
}
