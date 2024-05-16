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

package org.apache.iotdb.tsfile.read.common.block.column;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.RamUsageEstimator;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import java.util.Arrays;
import java.util.Optional;

import static org.apache.iotdb.tsfile.read.common.block.column.ColumnUtil.checkValidRegion;
import static org.apache.iotdb.tsfile.utils.RamUsageEstimator.sizeOfBooleanArray;

public class BooleanColumn implements Column {

  private static final int INSTANCE_SIZE =
      (int) RamUsageEstimator.shallowSizeOfInstance(BooleanColumn.class);
  public static final int SIZE_IN_BYTES_PER_POSITION = Byte.BYTES + Byte.BYTES;

  private final int arrayOffset;
  private final int positionCount;
  private final boolean[] valueIsNull;
  private final boolean[] values;

  private final long retainedSizeInBytes;

  public BooleanColumn(int positionCount, Optional<boolean[]> valueIsNull, boolean[] values) {
    this(0, positionCount, valueIsNull.orElse(null), values);
  }

  BooleanColumn(int arrayOffset, int positionCount, boolean[] valueIsNull, boolean[] values) {
    if (arrayOffset < 0) {
      throw new IllegalArgumentException("arrayOffset is negative");
    }
    this.arrayOffset = arrayOffset;
    if (positionCount < 0) {
      throw new IllegalArgumentException("positionCount is negative");
    }
    this.positionCount = positionCount;

    if (values.length - arrayOffset < positionCount) {
      throw new IllegalArgumentException("values length is less than positionCount");
    }
    this.values = values;

    if (valueIsNull != null && valueIsNull.length - arrayOffset < positionCount) {
      throw new IllegalArgumentException("isNull length is less than positionCount");
    }
    this.valueIsNull = valueIsNull;

    retainedSizeInBytes =
        INSTANCE_SIZE + sizeOfBooleanArray(positionCount) + sizeOfBooleanArray(positionCount);
  }

  @Override
  public TSDataType getDataType() {
    return TSDataType.BOOLEAN;
  }

  @Override
  public ColumnEncoding getEncoding() {
    return ColumnEncoding.BYTE_ARRAY;
  }

  @Override
  public boolean getBoolean(int position) {
    return values[position + arrayOffset];
  }

  @Override
  public boolean[] getBooleans() {
    return values;
  }

  @Override
  public Object getObject(int position) {
    return getBoolean(position);
  }

  @Override
  public TsPrimitiveType getTsPrimitiveType(int position) {
    return new TsPrimitiveType.TsBoolean(getBoolean(position));
  }

  @Override
  public boolean mayHaveNull() {
    return valueIsNull != null;
  }

  @Override
  public boolean isNull(int position) {
    return valueIsNull != null && valueIsNull[position + arrayOffset];
  }

  @Override
  public boolean[] isNull() {
    if (valueIsNull == null) {
      boolean[] res = new boolean[positionCount];
      Arrays.fill(res, false);
      return res;
    }
    return valueIsNull;
  }

  @Override
  public int getPositionCount() {
    return positionCount;
  }

  @Override
  public long getRetainedSizeInBytes() {
    return retainedSizeInBytes;
  }

  @Override
  public Column getRegion(int positionOffset, int length) {
    checkValidRegion(getPositionCount(), positionOffset, length);
    return new BooleanColumn(positionOffset + arrayOffset, length, valueIsNull, values);
  }

  @Override
  public Column getRegionCopy(int positionOffset, int length) {
    checkValidRegion(getPositionCount(), positionOffset, length);

    int from = positionOffset + arrayOffset;
    int to = from + length;
    boolean[] valueIsNullCopy =
        valueIsNull != null ? Arrays.copyOfRange(valueIsNull, from, to) : null;
    boolean[] valuesCopy = Arrays.copyOfRange(values, from, to);

    return new BooleanColumn(0, length, valueIsNullCopy, valuesCopy);
  }

  @Override
  public Column subColumn(int fromIndex) {
    if (fromIndex > positionCount) {
      throw new IllegalArgumentException("fromIndex is not valid");
    }
    return new BooleanColumn(
        arrayOffset + fromIndex, positionCount - fromIndex, valueIsNull, values);
  }

  @Override
  public Column subColumnCopy(int fromIndex) {
    if (fromIndex > positionCount) {
      throw new IllegalArgumentException("fromIndex is not valid");
    }

    int from = arrayOffset + fromIndex;
    boolean[] valueIsNullCopy =
        valueIsNull != null ? Arrays.copyOfRange(valueIsNull, from, positionCount) : null;
    boolean[] valuesCopy = Arrays.copyOfRange(values, from, positionCount);

    int length = positionCount - fromIndex;
    return new BooleanColumn(0, length, valueIsNullCopy, valuesCopy);
  }

  @Override
  public void reverse() {
    for (int i = arrayOffset, j = arrayOffset + positionCount - 1; i < j; i++, j--) {
      boolean valueTmp = values[i];
      values[i] = values[j];
      values[j] = valueTmp;
    }
    if (valueIsNull != null) {
      for (int i = arrayOffset, j = arrayOffset + positionCount - 1; i < j; i++, j--) {
        boolean isNullTmp = valueIsNull[i];
        valueIsNull[i] = valueIsNull[j];
        valueIsNull[j] = isNullTmp;
      }
    }
  }

  @Override
  public int getInstanceSize() {
    return INSTANCE_SIZE;
  }
}
