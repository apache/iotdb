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

package com.timecho.iotdb.calc.plan.relational.function.scalar.unary;

import org.apache.iotdb.calc.plan.planner.memory.MemoryReservationManager;
import org.apache.iotdb.calc.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.calc.transformation.dag.column.unary.UnaryColumnTransformer;
import org.apache.iotdb.calc.utils.ObjectTypeUtils;
import org.apache.iotdb.commons.exception.ObjectFileNotExist;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;

import java.util.Optional;

public class ReadObjectColumnTransformer extends UnaryColumnTransformer {

  private final Optional<MemoryReservationManager> memoryReservationManager;
  private long offset = 0;
  private long length = -1;

  public ReadObjectColumnTransformer(
      Type type,
      ColumnTransformer childColumnTransformer,
      Optional<MemoryReservationManager> memoryReservationManager) {
    super(type, childColumnTransformer);
    this.memoryReservationManager = memoryReservationManager;
  }

  public ReadObjectColumnTransformer(
      Type type,
      long offset,
      ColumnTransformer childColumnTransformer,
      Optional<MemoryReservationManager> memoryReservationManager) {
    super(type, childColumnTransformer);
    this.offset = offset;
    this.memoryReservationManager = memoryReservationManager;
  }

  public ReadObjectColumnTransformer(
      Type type,
      long offset,
      long length,
      ColumnTransformer childColumnTransformer,
      Optional<MemoryReservationManager> memoryReservationManager) {
    super(type, childColumnTransformer);
    this.offset = offset;
    this.length = length;
    this.memoryReservationManager = memoryReservationManager;
  }

  @Override
  protected void doTransform(Column column, ColumnBuilder columnBuilder) {
    for (int i = 0, n = column.getPositionCount(); i < n; i++) {
      if (!column.isNull(i)) {
        transform(
            column, columnBuilder, i, this.offset, this.length, this.memoryReservationManager);
      } else {
        columnBuilder.appendNull();
      }
    }
  }

  @Override
  protected void doTransform(Column column, ColumnBuilder columnBuilder, boolean[] selection) {
    for (int i = 0, n = column.getPositionCount(); i < n; i++) {
      if (selection[i] && !column.isNull(i)) {
        transform(
            column, columnBuilder, i, this.offset, this.length, this.memoryReservationManager);
      } else {
        columnBuilder.appendNull();
      }
    }
  }

  public static void transform(
      Column column,
      ColumnBuilder columnBuilder,
      int i,
      long offset,
      long length,
      Optional<MemoryReservationManager> memoryReservationManager) {
    // BinaryColumn.getDataType() returns TSDataType.TEXT
    if (TSDataType.TEXT == column.getDataType()) {
      Binary binary = column.getBinary(i);
      columnBuilder.writeBinary(readObject(binary, offset, length, memoryReservationManager));
    } else {
      throw new IllegalStateException("read_object function only accept a BinaryColumn.");
    }
  }

  public static Binary readObject(
      Binary binary,
      long offset,
      long length,
      Optional<MemoryReservationManager> memoryReservationManager) {
    Pair<Long, String> objectLengthPathPair =
        ObjectTypeUtils.parseObjectBinaryToSizeStringPathPair(binary);
    long fileLength = objectLengthPathPair.getLeft();
    String relativePath = objectLengthPathPair.getRight();
    int actualReadSize =
        ObjectTypeUtils.getActualReadSize(relativePath, fileLength, offset, length);
    memoryReservationManager.ifPresent(
        context -> context.reserveMemoryCumulatively(actualReadSize));
    try {
      return new Binary(
          ObjectTypeUtils.readObjectContent(relativePath, offset, actualReadSize, true).array());
    } catch (ObjectFileNotExist e) {
      return new Binary(new byte[0]);
    }
  }
}
