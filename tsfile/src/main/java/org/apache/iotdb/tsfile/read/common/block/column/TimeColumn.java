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

import org.openjdk.jol.info.ClassLayout;

import static io.airlift.slice.SizeOf.sizeOfLongArray;
import static org.apache.iotdb.tsfile.read.common.block.column.ColumnUtil.checkValidRegion;

public class TimeColumn implements Column {

  private static final int INSTANCE_SIZE = ClassLayout.parseClass(LongColumn.class).instanceSize();
  public static final int SIZE_IN_BYTES_PER_POSITION = Long.BYTES;

  private final int arrayOffset;
  private final int positionCount;

  private final long[] values;

  private final long retainedSizeInBytes;

  public TimeColumn(int positionCount, long[] values) {
    this(0, positionCount, values);
  }

  TimeColumn(int arrayOffset, int positionCount, long[] values) {
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

    retainedSizeInBytes = INSTANCE_SIZE + sizeOfLongArray(positionCount);
  }

  @Override
  public TSDataType getDataType() {
    return TSDataType.INT64;
  }

  @Override
  public ColumnEncoding getEncoding() {
    return ColumnEncoding.INT64_ARRAY;
  }

  @Override
  public long getLong(int position) {
    return values[position + arrayOffset];
  }

  public long getLongWithoutCheck(int position) {
    return values[position + arrayOffset];
  }

  @Override
  public Object getObject(int position) {
    return getLong(position);
  }

  public boolean mayHaveNull() {
    return false;
  }

  @Override
  public boolean isNull(int position) {
    return false;
  }

  @Override
  public boolean[] isNull() {
    // todo
    return null;
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
    return new TimeColumn(positionOffset + arrayOffset, length, values);
  }

  @Override
  public Column subColumn(int fromIndex) {
    if (fromIndex > positionCount) {
      throw new IllegalArgumentException("fromIndex is not valid");
    }
    return new TimeColumn(arrayOffset + fromIndex, positionCount - fromIndex, values);
  }

  @Override
  public void reverse() {
    for (int i = arrayOffset, j = arrayOffset + positionCount - 1; i < j; i++, j--) {
      long time = values[i];
      values[i] = values[j];
      values[j] = time;
    }
  }

  public long getStartTime() {
    return values[arrayOffset];
  }

  public long getEndTime() {
    return values[getPositionCount() + arrayOffset - 1];
  }

  public long[] getTimes() {
    return values;
  }

  @Override
  public int getInstanceSize() {
    return INSTANCE_SIZE;
  }
}
