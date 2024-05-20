/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iotdb.db.queryengine.transformation.dag.transformer.ternary;

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

public abstract class TernaryTransformer extends Transformer {
  protected final LayerReader firstReader;
  protected final LayerReader secondReader;
  protected final LayerReader thirdReader;

  protected final TSDataType firstReaderDataType;
  protected final TSDataType secondReaderDataType;
  protected final TSDataType thirdReaderDataType;

  protected final boolean isFirstReaderConstant;
  protected final boolean isSecondReaderConstant;
  protected final boolean isThirdReaderConstant;

  protected final boolean isCurrentConstant;

  protected Column[] firstColumns;
  protected Column[] secondColumns;
  protected Column[] thirdColumns;

  protected int firstConsumed;
  protected int secondConsumed;
  protected int thirdConsumed;

  protected TernaryTransformer(
      LayerReader firstReader, LayerReader secondReader, LayerReader thirdReader) {
    this.firstReader = firstReader;
    this.secondReader = secondReader;
    this.thirdReader = thirdReader;
    this.firstReaderDataType = firstReader.getDataTypes()[0];
    this.secondReaderDataType = secondReader.getDataTypes()[0];
    this.thirdReaderDataType = thirdReader.getDataTypes()[0];
    this.isFirstReaderConstant = firstReader.isConstantPointReader();
    this.isSecondReaderConstant = secondReader.isConstantPointReader();
    this.isThirdReaderConstant = thirdReader.isConstantPointReader();
    this.isCurrentConstant =
        isFirstReaderConstant && isSecondReaderConstant && isThirdReaderConstant;
    checkType();
  }

  @Override
  public YieldableState yieldValue() throws Exception {
    // Generate data
    if (firstColumns == null) {
      YieldableState state = firstReader.yield();
      if (state != YieldableState.YIELDABLE) {
        return state;
      }
      firstColumns = firstReader.current();
    }
    if (secondColumns == null) {
      YieldableState state = secondReader.yield();
      if (state != YieldableState.YIELDABLE) {
        return state;
      }
      secondColumns = secondReader.current();
    }
    if (thirdColumns == null) {
      YieldableState state = thirdReader.yield();
      if (state != YieldableState.YIELDABLE) {
        return state;
      }
      thirdColumns = thirdReader.current();
    }

    int firstCount = firstColumns[0].getPositionCount();
    int secondCount = secondColumns[0].getPositionCount();
    int thirdCount = thirdColumns[0].getPositionCount();
    int firstRemains = firstCount - firstConsumed;
    int secondRemains = secondCount - secondConsumed;
    int thirdRemains = thirdCount - thirdConsumed;

    int expectedEntries = Math.min(Math.min(firstRemains, secondRemains), thirdRemains);
    cachedColumns = mergeAndTransformColumns(expectedEntries);

    return YieldableState.YIELDABLE;
  }

  @Override
  public boolean isConstantPointReader() {
    return firstReader.isConstantPointReader()
        && secondReader.isConstantPointReader()
        && thirdReader.isConstantPointReader();
  }

  protected Column[] mergeAndTransformColumns(int count) throws QueryProcessException, IOException {
    TSDataType outputType = getDataTypes()[0];
    ColumnBuilder timeBuilder = new TimeColumnBuilder(null, count);
    ColumnBuilder valueBuilder = TypeUtils.initColumnBuilder(outputType, count);

    int firstEnd = firstColumns[0].getPositionCount();
    int secondEnd = secondColumns[0].getPositionCount();
    int thirdEnd = thirdColumns[0].getPositionCount();

    while (firstConsumed < firstEnd && secondConsumed < secondEnd && thirdConsumed < thirdEnd) {
      long time = findFirstSameTime();

      if (firstConsumed < firstEnd && secondConsumed < secondEnd && thirdConsumed < thirdEnd) {
        if (time != Long.MIN_VALUE) {
          timeBuilder.writeLong(time);
          if (firstColumns[0].isNull(firstConsumed)
              || secondColumns[0].isNull(secondConsumed)
              || thirdColumns[0].isNull(thirdConsumed)) {
            valueBuilder.appendNull();
          } else {
            transformAndCache(
                firstColumns[0],
                firstConsumed,
                secondColumns[0],
                secondConsumed,
                thirdColumns[0],
                thirdConsumed,
                valueBuilder);
          }
        }

        firstConsumed++;
        secondConsumed++;
        thirdConsumed++;
      }
    }

    // Clean up
    if (firstConsumed == firstEnd) {
      firstColumns = null;
      firstConsumed = 0;
      firstReader.consumedAll();
    }
    if (secondConsumed == secondEnd) {
      secondColumns = null;
      secondConsumed = 0;
      secondReader.consumedAll();
    }
    if (thirdConsumed == thirdEnd) {
      thirdColumns = null;
      thirdConsumed = 0;
      thirdReader.consumedAll();
    }

    Column times = timeBuilder.build();
    Column values = valueBuilder.build();
    return new Column[] {values, times};
  }

  private long findFirstSameTime() {
    int firstEnd = firstColumns[0].getPositionCount();
    int secondEnd = secondColumns[0].getPositionCount();
    int thirdEnd = thirdColumns[0].getPositionCount();

    long firstTime = getTime(firstReader, firstColumns, firstConsumed);
    long secondTime = getTime(secondReader, secondColumns, secondConsumed);
    long thirdTime = getTime(thirdReader, thirdColumns, thirdConsumed);

    while (firstTime != secondTime || secondTime != thirdTime) {
      if (firstTime < secondTime) {
        if (isFirstReaderConstant) {
          firstTime = secondTime;
        } else {
          firstConsumed++;
          if (firstConsumed < firstEnd) {
            firstTime = getTime(firstReader, firstColumns, firstConsumed);
          } else {
            break;
          }
        }
      } else if (secondTime < thirdTime) {
        if (isSecondReaderConstant) {
          secondTime = thirdTime;
        } else {
          secondConsumed++;
          if (secondConsumed < secondEnd) {
            secondTime = getTime(secondReader, secondColumns, secondConsumed);
          } else {
            break;
          }
        }
      } else {
        if (isThirdReaderConstant) {
          thirdTime = firstTime;
        } else {
          thirdConsumed++;
          if (thirdConsumed < thirdEnd) {
            thirdTime = getTime(thirdReader, thirdColumns, thirdConsumed);
          } else {
            break;
          }
        }
      }
    }

    return firstTime;
  }

  private long getTime(LayerReader reader, Column[] columns, int index) {
    return reader.isConstantPointReader() ? Long.MIN_VALUE : columns[1].getLong(index);
  }

  protected abstract void transformAndCache(
      Column firstValues,
      int firstIndex,
      Column secondValues,
      int secondIndex,
      Column thirdValues,
      int thirdIndex,
      ColumnBuilder builder)
      throws QueryProcessException, IOException;

  protected abstract void checkType();
}
