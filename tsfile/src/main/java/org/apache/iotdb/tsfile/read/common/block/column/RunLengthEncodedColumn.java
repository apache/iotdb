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
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;

import org.openjdk.jol.info.ClassLayout;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.iotdb.tsfile.read.common.block.column.ColumnUtil.checkValidRegion;

public class RunLengthEncodedColumn implements Column {

  private static final int INSTANCE_SIZE =
      ClassLayout.parseClass(RunLengthEncodedColumn.class).instanceSize();

  private final Column value;
  private final int positionCount;

  public RunLengthEncodedColumn(Column value, int positionCount) {
    requireNonNull(value, "value is null");
    if (value.getPositionCount() != 1) {
      throw new IllegalArgumentException(
          format(
              "Expected value to contain a single position but has %s positions",
              value.getPositionCount()));
    }

    if (value instanceof RunLengthEncodedColumn) {
      this.value = ((RunLengthEncodedColumn) value).getValue();
    } else {
      this.value = value;
    }

    if (positionCount < 0) {
      throw new IllegalArgumentException("positionCount is negative");
    }

    this.positionCount = positionCount;
  }

  public Column getValue() {
    return value;
  }

  @Override
  public TSDataType getDataType() {
    return value.getDataType();
  }

  @Override
  public ColumnEncoding getEncoding() {
    return value.getEncoding();
  }

  @Override
  public boolean getBoolean(int position) {
    checkReadablePosition(position);
    return value.getBoolean(position);
  }

  @Override
  public int getInt(int position) {
    checkReadablePosition(position);
    return value.getInt(position);
  }

  @Override
  public long getLong(int position) {
    checkReadablePosition(position);
    return value.getLong(position);
  }

  @Override
  public float getFloat(int position) {
    checkReadablePosition(position);
    return value.getFloat(position);
  }

  @Override
  public double getDouble(int position) {
    checkReadablePosition(position);
    return value.getDouble(position);
  }

  @Override
  public Binary getBinary(int position) {
    checkReadablePosition(position);
    return value.getBinary(position);
  }

  @Override
  public Object getObject(int position) {
    checkReadablePosition(position);
    return value.getObject(position);
  }

  @Override
  public TsPrimitiveType getTsPrimitiveType(int position) {
    checkReadablePosition(position);
    return value.getTsPrimitiveType(position);
  }

  @Override
  public boolean mayHaveNull() {
    return value.mayHaveNull();
  }

  @Override
  public boolean isNull(int position) {
    checkReadablePosition(position);
    return value.isNull(0);
  }

  @Override
  public int getPositionCount() {
    return positionCount;
  }

  @Override
  public long getRetainedSizeInBytes() {
    return INSTANCE_SIZE + value.getRetainedSizeInBytes();
  }

  @Override
  public Column getRegion(int positionOffset, int length) {
    checkValidRegion(positionCount, positionOffset, length);
    return new RunLengthEncodedColumn(value, length);
  }

  private void checkReadablePosition(int position) {
    if (position < 0 || position >= positionCount) {
      throw new IllegalArgumentException("position is not valid");
    }
  }
}
