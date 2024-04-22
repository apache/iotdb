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

package org.apache.iotdb.udf.api.utils;

import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.utils.Binary;

public class ResultValue {
  private ColumnBuilder builder;

  public ResultValue(ColumnBuilder builder) {
    this.builder = builder;
  }

  public ResultValue setBoolean(boolean value) {
    builder.writeBoolean(value);
    return this;
  }

  public ResultValue setInt(int value) {
    builder.writeInt(value);
    return this;
  }

  public ResultValue setLong(long value) {
    builder.writeLong(value);
    return this;
  }

  public ResultValue setFloat(float value) {
    builder.writeFloat(value);
    return this;
  }

  public ResultValue setDouble(double value) {
    builder.writeDouble(value);
    return this;
  }

  public ResultValue setBinary(Binary binary) {
    builder.writeBinary(binary);
    return this;
  }

  public ResultValue setObject(Object value) {
    builder.writeObject(value);
    return this;
  }

  public ResultValue setNull() {
    builder.appendNull();
    return this;
  }

  public ResultValue setNull(int nullCount) {
    builder.appendNull(nullCount);
    return this;
  }
}
