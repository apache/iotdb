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
import java.util.Map;

public class MeasurementColumnSchema extends TsTableColumnSchema {

  private final TSEncoding encoding;

  private final CompressionType compressor;

  public MeasurementColumnSchema(
      String columnName, TSDataType dataType, TSEncoding encoding, CompressionType compressor) {
    super(columnName, dataType);
    this.encoding = encoding;
    this.compressor = compressor;
  }

  public MeasurementColumnSchema(
      String columnName,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor,
      Map<String, String> props) {
    super(columnName, dataType, props);
    this.encoding = encoding;
    this.compressor = compressor;
  }

  @Override
  public TsTableColumnCategory getColumnCategory() {
    return TsTableColumnCategory.MEASUREMENT;
  }

  public TSEncoding getEncoding() {
    return encoding;
  }

  public CompressionType getCompressor() {
    return compressor;
  }

  public IMeasurementSchema getMeasurementSchema() {
    return new MeasurementSchema(columnName, dataType, encoding, compressor, props);
  }

  @Override
  void serialize(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(columnName, outputStream);
    ReadWriteIOUtils.write(dataType, outputStream);
    ReadWriteIOUtils.write(encoding, outputStream);
    ReadWriteIOUtils.write(compressor, outputStream);
    ReadWriteIOUtils.write(props, outputStream);
  }

  static MeasurementColumnSchema deserialize(InputStream stream) throws IOException {
    String columnName = ReadWriteIOUtils.readString(stream);
    TSDataType dataType = ReadWriteIOUtils.readDataType(stream);
    TSEncoding encoding = ReadWriteIOUtils.readEncoding(stream);
    CompressionType compressor = ReadWriteIOUtils.readCompressionType(stream);
    Map<String, String> props = ReadWriteIOUtils.readMap(stream);
    return new MeasurementColumnSchema(columnName, dataType, encoding, compressor, props);
  }

  static MeasurementColumnSchema deserialize(ByteBuffer buffer) {
    String columnName = ReadWriteIOUtils.readString(buffer);
    TSDataType dataType = ReadWriteIOUtils.readDataType(buffer);
    TSEncoding encoding = ReadWriteIOUtils.readEncoding(buffer);
    CompressionType compressor = ReadWriteIOUtils.readCompressionType(buffer);
    Map<String, String> props = ReadWriteIOUtils.readMap(buffer);
    return new MeasurementColumnSchema(columnName, dataType, encoding, compressor, props);
  }
}
