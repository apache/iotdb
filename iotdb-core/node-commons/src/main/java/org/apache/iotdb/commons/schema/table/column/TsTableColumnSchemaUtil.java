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

package org.apache.iotdb.commons.schema.table.column;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class TsTableColumnSchemaUtil {

  private TsTableColumnSchemaUtil() {
    // Do nothing
  }

  public static void serialize(TsTableColumnSchema columnSchema, OutputStream outputStream)
      throws IOException {
    ReadWriteIOUtils.write(columnSchema.getColumnCategory().getValue(), outputStream);
    columnSchema.serialize(outputStream);
  }

  public static TsTableColumnSchema deserialize(InputStream inputStream) throws IOException {
    return deserialize(TsTableColumnCategory.deserialize(inputStream), inputStream);
  }

  private static TsTableColumnSchema deserialize(TsTableColumnCategory category, InputStream stream)
      throws IOException {
    switch (category) {
      case ID:
        return IdColumnSchema.deserialize(stream);
      case ATTRIBUTE:
        return AttributeColumnSchema.deserialize(stream);
      case TIME:
        return TimeColumnSchema.deserialize(stream);
      case MEASUREMENT:
        return MeasurementColumnSchema.deserialize(stream);
      default:
        throw new IllegalArgumentException();
    }
  }

  public static TsTableColumnSchema deserialize(ByteBuffer buffer) {
    return deserialize(TsTableColumnCategory.deserialize(buffer), buffer);
  }

  private static TsTableColumnSchema deserialize(
      TsTableColumnCategory category, ByteBuffer buffer) {
    switch (category) {
      case ID:
        return IdColumnSchema.deserialize(buffer);
      case ATTRIBUTE:
        return AttributeColumnSchema.deserialize(buffer);
      case TIME:
        return TimeColumnSchema.deserialize(buffer);
      case MEASUREMENT:
        return MeasurementColumnSchema.deserialize(buffer);
      default:
        throw new IllegalArgumentException();
    }
  }

  public static byte[] serialize(List<TsTableColumnSchema> columnSchemaList) {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    try {
      serialize(columnSchemaList, stream);
    } catch (IOException ignored) {
      // ByteArrayOutputStream won't throw IOException
    }
    return stream.toByteArray();
  }

  public static void serialize(List<TsTableColumnSchema> columnSchemaList, OutputStream stream)
      throws IOException {
    if (columnSchemaList == null) {
      ReadWriteIOUtils.write(-1, stream);
      return;
    }
    ReadWriteIOUtils.write(columnSchemaList.size(), stream);
    for (TsTableColumnSchema columnSchema : columnSchemaList) {
      serialize(columnSchema, stream);
    }
  }

  public static List<TsTableColumnSchema> deserializeColumnSchemaList(ByteBuffer buffer) {
    int size = ReadWriteIOUtils.readInt(buffer);
    if (size == -1) {
      throw new IllegalArgumentException("size should not be -1");
    }
    List<TsTableColumnSchema> columnSchemaList = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      columnSchemaList.add(deserialize(buffer));
    }
    return columnSchemaList;
  }
}
