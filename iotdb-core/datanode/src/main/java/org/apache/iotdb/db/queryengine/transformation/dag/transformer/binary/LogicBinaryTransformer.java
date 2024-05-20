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

import org.apache.iotdb.db.queryengine.transformation.api.LayerReader;
import org.apache.iotdb.db.queryengine.transformation.api.YieldableState;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.column.BooleanColumn;
import org.apache.tsfile.read.common.block.column.BooleanColumnBuilder;
import org.apache.tsfile.read.common.block.column.RunLengthEncodedColumn;
import org.apache.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.util.Optional;

public abstract class LogicBinaryTransformer extends BinaryTransformer {
  private final int count = TSFileDescriptor.getInstance().getConfig().getMaxTsBlockLineNumber();

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
    // Generate data
    if (leftColumns == null) {
      YieldableState state = leftReader.yield();

      if (state == YieldableState.YIELDABLE) {
        leftColumns = leftReader.current();
      } else if (state == YieldableState.NOT_YIELDABLE_NO_MORE_DATA) {
        isLeftDone = true;
        if (isRightDone) {
          return YieldableState.NOT_YIELDABLE_NO_MORE_DATA;
        }
        // When another column has no data
        // Threat it as all false values
        boolean[] allFalse = new boolean[1];
        Column allFalseColumn =
            new RunLengthEncodedColumn(new BooleanColumn(1, Optional.empty(), allFalse), count);
        leftColumns = new Column[] {allFalseColumn};
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
        // When another column has no data
        // Threat it as all false values
        boolean[] allFalse = new boolean[1];
        Column allFalseColumn =
            new RunLengthEncodedColumn(new BooleanColumn(1, Optional.empty(), allFalse), count);
        rightColumns = new Column[] {allFalseColumn};
      } else {
        return YieldableState.NOT_YIELDABLE_WAITING_FOR_DATA;
      }
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

  @Override
  protected Column[] mergeAndTransformColumns(int count) {
    ColumnBuilder timeBuilder = new TimeColumnBuilder(null, count);
    ColumnBuilder valueBuilder = new BooleanColumnBuilder(null, count);

    if (isLeftReaderConstant || isRightReaderConstant) {
      return handleConstantColumns(valueBuilder);
    }

    return handleNonConstantColumns(timeBuilder, valueBuilder);
  }

  private Column[] handleNonConstantColumns(ColumnBuilder timeBuilder, ColumnBuilder valueBuilder) {
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
        boolean leftValue = !leftValues.isNull(leftConsumed) && leftValues.getBoolean(leftConsumed);
        boolean rightValue =
            !rightValues.isNull(rightConsumed) && rightValues.getBoolean(rightConsumed);
        boolean result = evaluate(leftValue, rightValue);
        valueBuilder.writeBoolean(result);

        leftConsumed++;
        rightConsumed++;
      }
    }

    // Clean up
    if (leftConsumed == leftEnd) {
      leftConsumed = 0;
      if (!isLeftDone) {
        leftColumns = null;
        leftReader.consumedAll();
      }
    }
    if (rightConsumed == rightEnd) {
      rightConsumed = 0;
      if (!isRightDone) {
        rightColumns = null;
        rightReader.consumedAll();
      }
    }

    Column times = timeBuilder.build();
    Column values = valueBuilder.build();
    return new Column[] {values, times};
  }

  private Column[] handleConstantColumns(ColumnBuilder valueBuilder) {
    if (isLeftReaderConstant && isRightReaderConstant) {
      boolean leftValue = leftColumns[0].getBoolean(0);
      boolean rightValue = rightColumns[0].getBoolean(0);
      boolean result = evaluate(leftValue, rightValue);
      valueBuilder.writeBoolean(result);

      return new Column[] {valueBuilder.build()};
    } else if (isLeftReaderConstant) {
      for (int i = 0; i < rightColumns[0].getPositionCount(); i++) {
        boolean leftValue = leftColumns[0].getBoolean(0);
        boolean rightValue = !rightColumns[0].isNull(i) && rightColumns[0].getBoolean(i);
        boolean result = evaluate(leftValue, rightValue);
        valueBuilder.writeBoolean(result);
      }
      Column times = rightColumns[1];
      Column values = valueBuilder.build();

      // Clean up
      rightColumns = null;
      rightReader.consumedAll();

      return new Column[] {values, times};
    } else if (isRightReaderConstant) {
      for (int i = 0; i < leftColumns[0].getPositionCount(); i++) {
        boolean leftValue = !leftColumns[0].isNull(0) && leftColumns[0].getBoolean(0);
        boolean rightValue = rightColumns[0].getBoolean(0);
        boolean result = evaluate(leftValue, rightValue);
        valueBuilder.writeBoolean(result);
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

  protected abstract boolean evaluate(boolean leftOperand, boolean rightOperand);

  @Override
  protected void transformAndCache(
      Column leftValues, int leftIndex, Column rightValues, int rightIndex, ColumnBuilder builder) {
    throw new UnsupportedOperationException();
  }

  @Override
  public TSDataType[] getDataTypes() {
    return new TSDataType[] {TSDataType.BOOLEAN};
  }
}
