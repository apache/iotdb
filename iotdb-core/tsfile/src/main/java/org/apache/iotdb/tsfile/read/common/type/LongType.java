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

package org.apache.iotdb.tsfile.read.common.type;

import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.LongColumnBuilder;

public class LongType implements Type {

  private static final LongType INSTANCE = new LongType();

  private LongType() {}

  @Override
  public int getInt(Column c, int position) {
    return (int) c.getLong(position);
  }

  @Override
  public long getLong(Column c, int position) {
    return c.getLong(position);
  }

  @Override
  public float getFloat(Column c, int position) {
    return c.getLong(position);
  }

  @Override
  public double getDouble(Column c, int position) {
    return c.getLong(position);
  }

  @Override
  public void writeInt(ColumnBuilder builder, int value) {
    builder.writeLong(value);
  }

  @Override
  public void writeLong(ColumnBuilder builder, long value) {
    builder.writeLong(value);
  }

  @Override
  public void writeFloat(ColumnBuilder builder, float value) {
    builder.writeLong((long) value);
  }

  @Override
  public void writeDouble(ColumnBuilder builder, double value) {
    builder.writeLong((long) value);
  }

  @Override
  public ColumnBuilder createColumnBuilder(int expectedEntries) {
    return new LongColumnBuilder(null, expectedEntries);
  }

  @Override
  public TypeEnum getTypeEnum() {
    return TypeEnum.INT64;
  }

  public static LongType getInstance() {
    return INSTANCE;
  }
}
