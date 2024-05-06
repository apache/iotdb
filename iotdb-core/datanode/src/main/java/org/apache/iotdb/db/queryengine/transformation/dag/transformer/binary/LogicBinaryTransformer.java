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
import org.apache.iotdb.db.queryengine.transformation.dag.util.TypeUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.column.BooleanColumn;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;

import java.io.IOException;
import java.util.Optional;

public abstract class LogicBinaryTransformer extends BinaryTransformer {
  private int count = TSFileDescriptor.getInstance().getConfig().getMaxTsBlockLineNumber();

  private boolean isLeftDone;
  private boolean isRightDone;

  protected LogicBinaryTransformer(LayerReader leftReader, LayerReader rightReader) {
    super(leftReader, rightReader);
  }

  @Override
  protected void checkType() {
    if (leftReaderDataType != TSDataType.BOOLEAN || rightReaderDataType != TSDataType.BOOLEAN) {
      throw new UnSupportedDataTypeException("Unsupported data type: " + TSDataType.BOOLEAN);
    }
  }

  @Override
  public YieldableState yieldValue() throws Exception {
    if (leftColumns == null) {
      YieldableState state = leftReader.yield();

      if (state == YieldableState.YIELDABLE) {
        leftColumns = leftReader.current();
      } else if (state == YieldableState.NOT_YIELDABLE_NO_MORE_DATA) {
        isLeftDone = true;
        if (isRightDone) {
          return YieldableState.NOT_YIELDABLE_NO_MORE_DATA;
        }

        boolean[] allFalse = new boolean[count];
        leftColumns = new Column[] {new BooleanColumn(count, Optional.empty(), allFalse)};
      } else {
        return YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA;
      }
    }

    if (rightColumns == null) {
      YieldableState state = rightReader.yield();

      if (state == YieldableState.YIELDABLE) {
        rightColumns = rightReader.current();
      } else if (state == YieldableState.NOT_YIELDABLE_NO_MORE_DATA) {
        isRightDone = true;
        if (isLeftDone) {
          return YieldableState.NOT_YIELDABLE_NO_MORE_DATA;
        }

        boolean[] allFalse = new boolean[count];
        rightColumns = new Column[] {new BooleanColumn(count, Optional.empty(), allFalse)};
      } else {
        return YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA;
      }
    }

    int leftCount = leftColumns[0].getPositionCount() - leftConsumed;
    int rightCount = rightColumns[0].getPositionCount() - rightConsumed;
    if (leftCount < rightCount) {
      // Consume all left columns
      cachedColumns = mergeAndTransformColumns(leftCount);
      // No need to call rightReader.consume()

      // Clean up
      leftConsumed = 0;
      if (!isLeftDone) {
        leftColumns = null;
        leftReader.consumedAll();
      }
    } else {
      // Consume all right columns
      cachedColumns = mergeAndTransformColumns(rightCount);
      // No need to call rightReader.consume()

      // Clean up right columns
      rightConsumed = 0;
      if (!isRightDone) {
        rightColumns = null;
        rightReader.consumedAll();
      }
    }

    return YieldableState.YIELDABLE;
  }

  @Override
  protected Column[] mergeAndTransformColumns(int count) throws QueryProcessException, IOException {
    // TODO: maybe we should choose more precise expectedEntries
    TSDataType outputType = getDataTypes()[0];
    ColumnBuilder timeBuilder = new TimeColumnBuilder(null, count);
    ColumnBuilder valueBuilder = TypeUtils.initColumnBuilder(outputType, count);

    if (isLeftReaderConstant || isRightReaderConstant) {
      int leftOffset = leftConsumed;
      int rightOffset = rightConsumed;
      for (int i = 0; i < count; i++) {
        boolean leftValue =
            !leftColumns[0].isNull(leftConsumed) && leftColumns[0].getBoolean(leftConsumed);
        boolean rightValue =
            !rightColumns[0].isNull(rightConsumed) && rightColumns[0].getBoolean(rightConsumed);
        boolean result = evaluate(leftValue, rightValue);
        valueBuilder.writeBoolean(result);

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
        boolean leftValue =
            !leftColumns[0].isNull(leftConsumed) && leftColumns[0].getBoolean(leftConsumed);
        boolean rightValue =
            !rightColumns[0].isNull(rightConsumed) && rightColumns[0].getBoolean(rightConsumed);
        boolean result = evaluate(leftValue, rightValue);
        valueBuilder.writeBoolean(result);

        leftConsumed++;
        rightConsumed++;
      }
    }

    Column times = timeBuilder.build();
    Column values = valueBuilder.build();
    return new Column[] {values, times};
  }

  protected abstract boolean evaluate(boolean leftOperand, boolean rightOperand);

  @Override
  protected void transformAndCache(
      Column leftValues, int leftIndex, Column rightValues, int rightIndex, ColumnBuilder builder)
      throws QueryProcessException, IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public TSDataType[] getDataTypes() {
    return new TSDataType[] {TSDataType.BOOLEAN};
  }
}
