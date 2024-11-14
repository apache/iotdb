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

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;

public class AttributeColumnSchema extends TsTableColumnSchema {

  private String originalName;

  public AttributeColumnSchema(final String columnName, final TSDataType dataType) {
    super(columnName, dataType);
  }

  public AttributeColumnSchema(
      final String columnName, final TSDataType dataType, final Map<String, String> props) {
    super(columnName, dataType, props);
  }

  public void setOriginalName(final String originalName) {
    this.originalName = originalName;
  }

  public String getOriginalName() {
    return originalName;
  }

  @Override
  public TsTableColumnCategory getColumnCategory() {
    return TsTableColumnCategory.ATTRIBUTE;
  }

  @Override
  void serialize(final OutputStream outputStream) throws IOException {
    super.serialize(outputStream);
    ReadWriteIOUtils.write(originalName, outputStream);
  }

  static AttributeColumnSchema deserialize(final InputStream stream) throws IOException {
    final String columnName = ReadWriteIOUtils.readString(stream);
    final TSDataType dataType = ReadWriteIOUtils.readDataType(stream);
    final Map<String, String> props = ReadWriteIOUtils.readMap(stream);
    final AttributeColumnSchema schema = new AttributeColumnSchema(columnName, dataType, props);
    final String originalName = ReadWriteIOUtils.readString(stream);
    schema.setOriginalName(originalName);
    return schema;
  }

  static AttributeColumnSchema deserialize(final ByteBuffer buffer) {
    final String columnName = ReadWriteIOUtils.readString(buffer);
    final TSDataType dataType = ReadWriteIOUtils.readDataType(buffer);
    final Map<String, String> props = ReadWriteIOUtils.readMap(buffer);
    final AttributeColumnSchema schema = new AttributeColumnSchema(columnName, dataType, props);
    final String originalName = ReadWriteIOUtils.readString(buffer);
    schema.setOriginalName(originalName);
    return schema;
  }
}
