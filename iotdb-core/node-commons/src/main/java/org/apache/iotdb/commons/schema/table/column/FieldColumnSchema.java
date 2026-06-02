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
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class FieldColumnSchema extends TsTableColumnSchema {

  private TSEncoding encoding;

  private final CompressionType compressor;

  // Only for information schema and tree view field
  public FieldColumnSchema(final String columnName, final TSDataType dataType) {
    super(columnName, dataType);
    this.encoding = TSEncoding.PLAIN;
    this.compressor = CompressionType.UNCOMPRESSED;
  }

  public FieldColumnSchema(
      final String columnName,
      final TSDataType dataType,
      final TSEncoding encoding,
      final CompressionType compressor) {
    super(columnName, dataType);
    this.encoding = encoding;
    this.compressor = compressor;
  }

  public FieldColumnSchema(
      final String columnName,
      final TSDataType dataType,
      final TSEncoding encoding,
      final CompressionType compressor,
      final Map<String, String> props) {
    super(columnName, dataType, props);
    this.encoding = encoding;
    this.compressor = compressor;
  }

  // Only for table view
  @Override
  public void setDataType(final TSDataType dataType) {
    this.dataType = dataType;
  }

  @Override
  public TsTableColumnCategory getColumnCategory() {
    return TsTableColumnCategory.FIELD;
  }

  public TSEncoding getEncoding() {
    return encoding;
  }

  public void setEncoding(TSEncoding encoding) {
    this.encoding = encoding;
  }

  public CompressionType getCompressor() {
    return compressor;
  }

  @Override
  public IMeasurementSchema getMeasurementSchema() {
    return new MeasurementSchema(columnName, dataType, encoding, compressor, props);
  }

  @Override
  void serialize(final OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(columnName, outputStream);
    ReadWriteIOUtils.write(dataType, outputStream);
    ReadWriteIOUtils.write(encoding, outputStream);
    ReadWriteIOUtils.write(compressor, outputStream);
    ReadWriteIOUtils.write(props, outputStream);
  }

  static FieldColumnSchema deserialize(final InputStream stream) throws IOException {
    final String columnName = ReadWriteIOUtils.readString(stream);
    final TSDataType dataType = ReadWriteIOUtils.readDataType(stream);
    final TSEncoding encoding = ReadWriteIOUtils.readEncoding(stream);
    final CompressionType compressor = ReadWriteIOUtils.readCompressionType(stream);
    final Map<String, String> props = ReadWriteIOUtils.readMap(stream);
    return new FieldColumnSchema(columnName, dataType, encoding, compressor, props);
  }

  static FieldColumnSchema deserialize(final ByteBuffer buffer) {
    final String columnName = ReadWriteIOUtils.readString(buffer);
    final TSDataType dataType = ReadWriteIOUtils.readDataType(buffer);
    final TSEncoding encoding = ReadWriteIOUtils.readEncoding(buffer);
    final CompressionType compressor = ReadWriteIOUtils.readCompressionType(buffer);
    final Map<String, String> props = ReadWriteIOUtils.readMap(buffer);
    return new FieldColumnSchema(columnName, dataType, encoding, compressor, props);
  }

  @Override
  public TsTableColumnSchema copy() {
    return new FieldColumnSchema(
        columnName, dataType, encoding, compressor, props == null ? null : new HashMap<>(props));
  }
}
