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

package org.apache.iotdb.tsfile.write.schema;

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.encoding.encoder.TSEncodingBuilder;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.utils.StringContainer;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * TimeseriesSchema is like MeasurementSchema, but instead of measurementId, it stores the full
 * path.
 */
public class TimeseriesSchema implements Comparable<TimeseriesSchema>, Serializable {
  private String fullPath;
  private TSDataType type;
  private TSEncoding encoding;
  private transient TSEncodingBuilder encodingConverter;
  private CompressionType compressor;
  private Map<String, String> props = new HashMap<>();

  public TimeseriesSchema() {}

  public TimeseriesSchema(String fullPath, TSDataType tsDataType) {
    this(
        fullPath,
        tsDataType,
        TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getValueEncoder()),
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
  }

  /** set properties as an empty Map. */
  public TimeseriesSchema(String fullPath, TSDataType type, TSEncoding encoding) {
    this(
        fullPath,
        type,
        encoding,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        Collections.emptyMap());
  }

  public TimeseriesSchema(
      String fullPath, TSDataType type, TSEncoding encoding, CompressionType compressionType) {
    this(fullPath, type, encoding, compressionType, Collections.emptyMap());
  }

  /**
   * Constructor of TimeseriesSchema.
   *
   * <p>props - information in encoding method. For RLE, Encoder.MAX_POINT_NUMBER For PLAIN,
   * Encoder.maxStringLength
   */
  public TimeseriesSchema(
      String fullPath,
      TSDataType type,
      TSEncoding encoding,
      CompressionType compressionType,
      Map<String, String> props) {
    this.type = type;
    this.fullPath = fullPath;
    this.encoding = encoding;
    this.props = props == null ? Collections.emptyMap() : props;
    this.compressor = compressionType;
  }

  /** function for deserializing data from byte buffer. */
  public static TimeseriesSchema deserializeFrom(ByteBuffer buffer) {
    TimeseriesSchema timeseriesSchema = new TimeseriesSchema();

    timeseriesSchema.fullPath = ReadWriteIOUtils.readString(buffer);

    timeseriesSchema.type = ReadWriteIOUtils.readDataType(buffer);

    timeseriesSchema.encoding = ReadWriteIOUtils.readEncoding(buffer);

    timeseriesSchema.compressor = ReadWriteIOUtils.readCompressionType(buffer);

    int size = ReadWriteIOUtils.readInt(buffer);
    if (size > 0) {
      timeseriesSchema.props = new HashMap<>();
      String key;
      String value;
      for (int i = 0; i < size; i++) {
        key = ReadWriteIOUtils.readString(buffer);
        value = ReadWriteIOUtils.readString(buffer);
        timeseriesSchema.props.put(key, value);
      }
    }

    return timeseriesSchema;
  }

  public String getFullPath() {
    return fullPath;
  }

  public void setFullPath(String fullPath) {
    this.fullPath = fullPath;
  }

  public Map<String, String> getProps() {
    return props;
  }

  public TSEncoding getEncodingType() {
    return encoding;
  }

  public TSDataType getType() {
    return type;
  }

  public void setProps(Map<String, String> props) {
    this.props = props;
  }

  /** function for getting time encoder. */
  public Encoder getTimeEncoder() {
    TSEncoding timeEncoding =
        TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder());
    TSDataType timeType = TSFileDescriptor.getInstance().getConfig().getTimeSeriesDataType();
    return TSEncodingBuilder.getEncodingBuilder(timeEncoding).getEncoder(timeType);
  }

  /**
   * get Encoder of value from encodingConverter by measurementID and data type.
   *
   * @return Encoder for value
   */
  public Encoder getValueEncoder() {
    // it is ok even if encodingConverter is constructed two instances for concurrent scenario
    if (encodingConverter == null) {
      // initialize TSEncoding. e.g. set max error for PLA and SDT
      encodingConverter = TSEncodingBuilder.getEncodingBuilder(encoding);
      encodingConverter.initFromProps(props);
    }
    return encodingConverter.getEncoder(type);
  }

  public CompressionType getCompressor() {
    return compressor;
  }

  /** function for serializing data to output stream. */
  public int serializeTo(OutputStream outputStream) throws IOException {
    int byteLen = 0;

    byteLen += ReadWriteIOUtils.write(fullPath, outputStream);

    byteLen += ReadWriteIOUtils.write(type, outputStream);

    byteLen += ReadWriteIOUtils.write(encoding, outputStream);

    byteLen += ReadWriteIOUtils.write(compressor, outputStream);

    if (props == null) {
      byteLen += ReadWriteIOUtils.write(0, outputStream);
    } else {
      byteLen += ReadWriteIOUtils.write(props.size(), outputStream);
      for (Map.Entry<String, String> entry : props.entrySet()) {
        byteLen += ReadWriteIOUtils.write(entry.getKey(), outputStream);
        byteLen += ReadWriteIOUtils.write(entry.getValue(), outputStream);
      }
    }

    return byteLen;
  }

  /** function for serializing data to byte buffer. */
  public int serializeTo(ByteBuffer buffer) {
    int byteLen = 0;

    byteLen += ReadWriteIOUtils.write(fullPath, buffer);

    byteLen += ReadWriteIOUtils.write(type, buffer);

    byteLen += ReadWriteIOUtils.write(encoding, buffer);

    byteLen += ReadWriteIOUtils.write(compressor, buffer);

    if (props == null) {
      byteLen += ReadWriteIOUtils.write(0, buffer);
    } else {
      byteLen += ReadWriteIOUtils.write(props.size(), buffer);
      for (Map.Entry<String, String> entry : props.entrySet()) {
        byteLen += ReadWriteIOUtils.write(entry.getKey(), buffer);
        byteLen += ReadWriteIOUtils.write(entry.getValue(), buffer);
      }
    }

    return byteLen;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TimeseriesSchema that = (TimeseriesSchema) o;
    return type == that.type
        && encoding == that.encoding
        && Objects.equals(fullPath, that.fullPath)
        && Objects.equals(compressor, that.compressor);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, encoding, fullPath, compressor);
  }

  /** compare by full path. */
  @Override
  public int compareTo(TimeseriesSchema o) {
    if (equals(o)) {
      return 0;
    } else {
      return this.fullPath.compareTo(o.fullPath);
    }
  }

  @Override
  public String toString() {
    StringContainer sc = new StringContainer("");
    sc.addTail(
        "[",
        fullPath,
        ",",
        type.toString(),
        ",",
        encoding.toString(),
        ",",
        props.toString(),
        ",",
        compressor.toString());
    sc.addTail("]");
    return sc.toString();
  }
}
