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
import org.apache.iotdb.tsfile.utils.Binary;

public class LongType implements Type {
  @Override
  public boolean getBoolean(Column c, int position) {
    return Type.super.getBoolean(c, position);
  }

  @Override
  public int getInt(Column c, int position) {
    return Type.super.getInt(c, position);
  }

  @Override
  public long getLong(Column c, int position) {
    return Type.super.getLong(c, position);
  }

  @Override
  public float getFloat(Column c, int position) {
    return Type.super.getFloat(c, position);
  }

  @Override
  public double getDouble(Column c, int position) {
    return Type.super.getDouble(c, position);
  }

  @Override
  public Binary getBinary(Column c, int position) {
    return Type.super.getBinary(c, position);
  }

  @Override
  public void writeBoolean(ColumnBuilder builder, boolean value) {
    Type.super.writeBoolean(builder, value);
  }

  @Override
  public void writeInt(ColumnBuilder builder, int value) {
    Type.super.writeInt(builder, value);
  }

  @Override
  public void writeLong(ColumnBuilder builder, long value) {
    Type.super.writeLong(builder, value);
  }

  @Override
  public void writeFloat(ColumnBuilder builder, float value) {
    Type.super.writeFloat(builder, value);
  }

  @Override
  public void writeDouble(ColumnBuilder builder, double value) {
    Type.super.writeDouble(builder, value);
  }

  @Override
  public void writeBinary(ColumnBuilder builder, Binary value) {
    Type.super.writeBinary(builder, value);
  }

  @Override
  public void appendNull(ColumnBuilder builder) {
    Type.super.appendNull(builder);
  }

  @Override
  public ColumnBuilder createBlockBuilder(int expectedEntries) {
    return null;
  }
}
