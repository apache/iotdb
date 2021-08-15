/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.query.dataset;

import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

public class ShowTimeSeriesResult extends ShowResult {

  private String alias;
  private TSDataType dataType;
  private TSEncoding encoding;
  private CompressionType compressor;
  private Map<String, String> tags;
  private Map<String, String> attributes;

  public ShowTimeSeriesResult(
      String name,
      String alias,
      String sgName,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor,
      Map<String, String> tags,
      Map<String, String> attributes) {
    super(name, sgName);
    this.alias = alias;
    this.dataType = dataType;
    this.encoding = encoding;
    this.compressor = compressor;
    this.tags = tags;
    this.attributes = attributes;
  }

  public ShowTimeSeriesResult() {
    super();
  }

  public String getAlias() {
    return alias;
  }

  public TSDataType getDataType() {
    return dataType;
  }

  public TSEncoding getEncoding() {
    return encoding;
  }

  public CompressionType getCompressor() {
    return compressor;
  }

  public Map<String, String> getTag() {
    return tags;
  }

  public Map<String, String> getAttribute() {
    return attributes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ShowTimeSeriesResult result = (ShowTimeSeriesResult) o;
    return Objects.equals(name, result.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }

  private void writeNullable(Map<String, String> param, OutputStream outputStream)
      throws IOException {
    ReadWriteIOUtils.write(param != null, outputStream);
    if (param != null) {
      ReadWriteIOUtils.write(param.size(), outputStream);
      for (Entry<String, String> entry : param.entrySet()) {
        ReadWriteIOUtils.write(entry.getKey(), outputStream);
        ReadWriteIOUtils.write(entry.getValue(), outputStream);
      }
    }
  }

  public void serialize(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(name, outputStream);
    ReadWriteIOUtils.write(alias != null, outputStream); // flag
    if (alias != null) {
      ReadWriteIOUtils.write(alias, outputStream);
    }
    ReadWriteIOUtils.write(sgName, outputStream);
    ReadWriteIOUtils.write(dataType, outputStream);
    ReadWriteIOUtils.write(encoding, outputStream);
    ReadWriteIOUtils.write(compressor, outputStream);

    // flag for tags and attributes
    writeNullable(tags, outputStream);
    writeNullable(attributes, outputStream);
  }

  public static ShowTimeSeriesResult deserialize(ByteBuffer buffer) {
    ShowTimeSeriesResult result = new ShowTimeSeriesResult();
    result.name = ReadWriteIOUtils.readString(buffer);
    if (buffer.get() == 1) { // flag
      result.alias = ReadWriteIOUtils.readString(buffer);
    }
    result.sgName = ReadWriteIOUtils.readString(buffer);
    result.dataType = ReadWriteIOUtils.readDataType(buffer);
    result.encoding = ReadWriteIOUtils.readEncoding(buffer);
    result.compressor = ReadWriteIOUtils.readCompressionType(buffer);

    // flag for tag
    if (buffer.get() == 1) {
      int tagSize = buffer.getInt();
      result.tags = new HashMap<>(tagSize);
      for (int i = 0; i < tagSize; i++) {
        String key = ReadWriteIOUtils.readString(buffer);
        String value = ReadWriteIOUtils.readString(buffer);
        result.tags.put(key, value);
      }
    }

    // flag for attribute
    if (buffer.get() == 1) {
      int attributeSize = buffer.getInt();
      result.attributes = new HashMap<>(attributeSize);
      for (int i = 0; i < attributeSize; i++) {
        String key = ReadWriteIOUtils.readString(buffer);
        String value = ReadWriteIOUtils.readString(buffer);
        result.attributes.put(key, value);
      }
    }
    return result;
  }
}
