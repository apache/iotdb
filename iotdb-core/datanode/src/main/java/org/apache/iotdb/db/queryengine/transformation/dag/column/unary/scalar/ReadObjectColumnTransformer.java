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

package org.apache.iotdb.db.queryengine.transformation.dag.column.unary.scalar;

import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.UnaryColumnTransformer;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.type.Type;
import org.apache.tsfile.utils.Binary;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Optional;

public class ReadObjectColumnTransformer extends UnaryColumnTransformer {

  private final Optional<FragmentInstanceContext> fragmentInstanceContext;
  private long offset = 0;
  private long length = -1;

  public ReadObjectColumnTransformer(
      Type type,
      ColumnTransformer childColumnTransformer,
      Optional<FragmentInstanceContext> fragmentInstanceContext) {
    super(type, childColumnTransformer);
    this.fragmentInstanceContext = fragmentInstanceContext;
  }

  public ReadObjectColumnTransformer(
      Type type,
      long offset,
      ColumnTransformer childColumnTransformer,
      Optional<FragmentInstanceContext> fragmentInstanceContext) {
    super(type, childColumnTransformer);
    this.offset = offset;
    this.fragmentInstanceContext = fragmentInstanceContext;
  }

  public ReadObjectColumnTransformer(
      Type type,
      long offset,
      long length,
      ColumnTransformer childColumnTransformer,
      Optional<FragmentInstanceContext> fragmentInstanceContext) {
    super(type, childColumnTransformer);
    this.offset = offset;
    this.length = length;
    this.fragmentInstanceContext = fragmentInstanceContext;
  }

  @Override
  protected void doTransform(Column column, ColumnBuilder columnBuilder) {
    for (int i = 0, n = column.getPositionCount(); i < n; i++) {
      if (!column.isNull(i)) {
        transform(column, columnBuilder, i);
      } else {
        columnBuilder.appendNull();
      }
    }
  }

  @Override
  protected void doTransform(Column column, ColumnBuilder columnBuilder, boolean[] selection) {
    for (int i = 0, n = column.getPositionCount(); i < n; i++) {
      if (selection[i] && !column.isNull(i)) {
        transform(column, columnBuilder, i);
      } else {
        columnBuilder.appendNull();
      }
    }
  }

  private void transform(Column column, ColumnBuilder columnBuilder, int i) {
    if (TSDataType.TEXT == column.getDataType()) {
      Binary binary = column.getBinary(i);
      columnBuilder.writeBinary(readObject(binary));
    }
  }

  private Binary readObject(Binary binary) {
    File file = new File(getObjectPathFromBinary(binary));
    // TODO: allocate memory
    long fileSize = file.length();
    if (offset >= fileSize) {
      throw new UnsupportedOperationException("offset is greater than object size");
    }
    long actualReadSize = Math.min(length < 0 ? fileSize : length, fileSize - offset);
    if (actualReadSize > Integer.MAX_VALUE) {
      throw new UnsupportedOperationException("Read object size is too large (size > 2G)");
    }
    fragmentInstanceContext.ifPresent(
        context -> context.getMemoryReservationContext().reserveMemoryCumulatively(actualReadSize));
    byte[] bytes = new byte[(int) actualReadSize];
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    try (FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
      fileChannel.read(buffer);
    } catch (IOException e) {
      throw new IoTDBRuntimeException(e, TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }
    return new Binary(bytes);
  }

  private String getObjectPathFromBinary(Binary binary) {
    return binary.toString().split(",")[0];
  }
}
