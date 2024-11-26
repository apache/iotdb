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
package org.apache.tsfile.write.schema;

import org.apache.tsfile.common.TsFileApi;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.encoding.encoder.Encoder;
import org.apache.tsfile.encoding.encoder.TSEncodingBuilder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.utils.StringContainer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * This class describes a measurement's information registered in {@linkplain
 * org.apache.tsfile.write.schema.Schema FileSchema}, including measurement id, data type, encoding
 * and compressor type. For each TSEncoding, MeasurementSchema maintains respective
 * TSEncodingBuilder; For TSDataType, only ENUM has TSDataTypeConverter up to now.
 */
public class MeasurementSchema
    implements IMeasurementSchema, Comparable<MeasurementSchema>, Serializable {

  private String measurementName;
  private TSDataType dataType;
  private TSEncoding encoding;
  private CompressionType compressionType;
  private TSEncodingBuilder encodingConverter;
  private Map<String, String> props = null;

  public MeasurementSchema() {}

  @TsFileApi
  public MeasurementSchema(String measurementName, TSDataType dataType) {
    this(
        measurementName,
        dataType,
        TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getValueEncoder()),
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        null);
  }

  /** set properties as an empty Map. */
  public MeasurementSchema(String measurementName, TSDataType dataType, TSEncoding encoding) {
    this(
        measurementName,
        dataType,
        encoding,
        TSFileDescriptor.getInstance().getConfig().getCompressor(),
        null);
  }

  @TsFileApi
  public MeasurementSchema(
      String measurementName,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressionType) {
    this(measurementName, dataType, encoding, compressionType, null);
  }

  /**
   * Constructor of MeasurementSchema.
   *
   * <p>props - information in encoding method. For RLE, Encoder.MAX_POINT_NUMBER For PLAIN,
   * Encoder.maxStringLength
   */
  public MeasurementSchema(
      String measurementName,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressionType,
      Map<String, String> props) {
    this.dataType = dataType;
    this.measurementName = measurementName;
    this.encoding = encoding;
    this.props = props;
    this.compressionType = compressionType;
  }

  public MeasurementSchema(
      String measurementName,
      byte type,
      byte encoding,
      byte compressor,
      Map<String, String> props) {
    this.dataType = TSDataType.getTsDataType(type);
    this.measurementName = measurementName;
    this.encoding = TSEncoding.deserialize(encoding);
    this.props = props;
    this.compressionType = CompressionType.deserialize(compressor);
  }

  /** function for deserializing data from input stream. */
  public static MeasurementSchema deserializeFrom(InputStream inputStream) throws IOException {
    MeasurementSchema measurementSchema = new MeasurementSchema();

    measurementSchema.measurementName = ReadWriteIOUtils.readString(inputStream);

    measurementSchema.dataType = TSDataType.deserializeFrom(inputStream);

    measurementSchema.encoding = TSEncoding.deserialize(ReadWriteIOUtils.readByte(inputStream));

    measurementSchema.compressionType =
        CompressionType.deserialize(ReadWriteIOUtils.readByte(inputStream));

    int size = ReadWriteIOUtils.readInt(inputStream);
    if (size > 0) {
      measurementSchema.props = new HashMap<>();
      String key;
      String value;
      for (int i = 0; i < size; i++) {
        key = ReadWriteIOUtils.readString(inputStream);
        value = ReadWriteIOUtils.readString(inputStream);
        measurementSchema.props.put(key, value);
      }
    }

    return measurementSchema;
  }

  /** function for deserializing data from byte buffer. */
  public static MeasurementSchema deserializeFrom(ByteBuffer buffer) {
    MeasurementSchema measurementSchema = new MeasurementSchema();

    measurementSchema.measurementName = ReadWriteIOUtils.readString(buffer);

    measurementSchema.dataType = TSDataType.deserializeFrom(buffer);

    measurementSchema.encoding = TSEncoding.deserialize(ReadWriteIOUtils.readByte(buffer));

    measurementSchema.compressionType =
        CompressionType.deserialize(ReadWriteIOUtils.readByte(buffer));

    int size = ReadWriteIOUtils.readInt(buffer);
    if (size > 0) {
      measurementSchema.props = new HashMap<>();
      String key;
      String value;
      for (int i = 0; i < size; i++) {
        key = ReadWriteIOUtils.readString(buffer);
        value = ReadWriteIOUtils.readString(buffer);
        measurementSchema.props.put(key, value);
      }
    }

    return measurementSchema;
  }

  public static MeasurementSchema partialDeserializeFrom(ByteBuffer buffer) {
    MeasurementSchema measurementSchema = new MeasurementSchema();

    measurementSchema.measurementName = ReadWriteIOUtils.readString(buffer);

    measurementSchema.dataType = TSDataType.deserializeFrom(buffer);

    measurementSchema.encoding = TSEncoding.deserialize(ReadWriteIOUtils.readByte(buffer));

    measurementSchema.compressionType =
        CompressionType.deserialize(ReadWriteIOUtils.readByte(buffer));

    return measurementSchema;
  }

  @Override
  public MeasurementSchemaType getSchemaType() {
    return MeasurementSchemaType.MEASUREMENT_SCHEMA;
  }

  @Override
  public String getMeasurementName() {
    return measurementName;
  }

  public void setMeasurementName(String measurementName) {
    this.measurementName = measurementName;
  }

  @Override
  public Map<String, String> getProps() {
    return props;
  }

  @Override
  public TSEncoding getEncodingType() {
    return encoding;
  }

  @Override
  public TSDataType getType() {
    return dataType;
  }

  @Override
  public byte getTypeInByte() {
    return dataType.serialize();
  }

  @Override
  public TSEncoding getTimeTSEncoding() {
    return TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder());
  }

  public void setProps(Map<String, String> props) {
    this.props = props;
  }

  /** function for getting time encoder. */
  @Override
  public Encoder getTimeEncoder() {
    TSEncoding timeEncoding =
        TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder());
    TSDataType timeType = TSFileDescriptor.getInstance().getConfig().getTimeSeriesDataType();
    return TSEncodingBuilder.getEncodingBuilder(timeEncoding).getEncoder(timeType);
  }

  @Override
  public List<String> getSubMeasurementsList() {
    throw new UnsupportedOperationException("unsupported method for MeasurementSchema");
  }

  @Override
  public List<TSDataType> getSubMeasurementsTSDataTypeList() {
    throw new UnsupportedOperationException("unsupported method for MeasurementSchema");
  }

  @Override
  public List<TSEncoding> getSubMeasurementsTSEncodingList() {
    throw new UnsupportedOperationException("unsupported method for MeasurementSchema");
  }

  @Override
  public List<Encoder> getSubMeasurementsEncoderList() {
    throw new UnsupportedOperationException("unsupported method for MeasurementSchema");
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
    return encodingConverter.getEncoder(dataType);
  }

  @Override
  public CompressionType getCompressor() {
    return this.compressionType;
  }

  /** function for serializing data to output stream. */
  @Override
  public int serializeTo(OutputStream outputStream) throws IOException {
    int byteLen = 0;

    byteLen += ReadWriteIOUtils.write(measurementName, outputStream);

    byteLen += ReadWriteIOUtils.write(dataType.serialize(), outputStream);

    byteLen += ReadWriteIOUtils.write(encoding, outputStream);

    byteLen += ReadWriteIOUtils.write(compressionType.serialize(), outputStream);

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

  @Override
  public int serializedSize() {
    int byteLen = 0;
    byteLen += ReadWriteIOUtils.sizeToWrite(measurementName);
    byteLen += 3 * Byte.BYTES;
    if (props == null) {
      byteLen += Integer.BYTES;
    } else {
      byteLen += Integer.BYTES;
      for (Map.Entry<String, String> entry : props.entrySet()) {
        byteLen += ReadWriteIOUtils.sizeToWrite(entry.getKey());
        byteLen += ReadWriteIOUtils.sizeToWrite(entry.getValue());
      }
    }

    return byteLen;
  }

  /** function for serializing data to byte buffer. */
  @Override
  public int serializeTo(ByteBuffer buffer) {
    int byteLen = 0;

    byteLen += ReadWriteIOUtils.write(measurementName, buffer);

    byteLen += ReadWriteIOUtils.write(dataType.serialize(), buffer);

    byteLen += ReadWriteIOUtils.write(encoding, buffer);

    byteLen += ReadWriteIOUtils.write(compressionType.serialize(), buffer);

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
  public int partialSerializeTo(OutputStream outputStream) throws IOException {
    int byteLen = 0;

    byteLen += ReadWriteIOUtils.write((byte) 0, outputStream);
    byteLen += ReadWriteIOUtils.write(measurementName, outputStream);
    byteLen += ReadWriteIOUtils.write(dataType.serialize(), outputStream);
    byteLen += ReadWriteIOUtils.write(encoding, outputStream);
    byteLen += ReadWriteIOUtils.write(compressionType.serialize(), outputStream);

    return byteLen;
  }

  @Override
  public boolean isLogicalView() {
    return false;
  }

  @Override
  public int partialSerializeTo(ByteBuffer buffer) {
    int byteLen = 0;

    byteLen += ReadWriteIOUtils.write((byte) 0, buffer);
    byteLen += ReadWriteIOUtils.write(measurementName, buffer);
    byteLen += ReadWriteIOUtils.write(dataType.serialize(), buffer);
    byteLen += ReadWriteIOUtils.write(encoding, buffer);
    byteLen += ReadWriteIOUtils.write(compressionType.serialize(), buffer);

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
    MeasurementSchema that = (MeasurementSchema) o;
    return dataType == that.dataType
        && encoding == that.encoding
        && Objects.equals(measurementName, that.measurementName)
        && compressionType == that.compressionType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(dataType, encoding, measurementName, compressionType);
  }

  /** compare by measurementID. */
  @Override
  public int compareTo(MeasurementSchema o) {
    if (equals(o)) {
      return 0;
    } else {
      return this.measurementName.compareTo(o.measurementName);
    }
  }

  @Override
  public String toString() {
    StringContainer sc = new StringContainer("");
    sc.addTail(
        "[",
        measurementName,
        ",",
        dataType.toString(),
        ",",
        encoding.toString(),
        ",",
        props == null ? "" : props.toString(),
        ",",
        compressionType.toString());
    sc.addTail("]");
    return sc.toString();
  }

  public void setDataType(TSDataType dataType) {
    this.dataType = dataType;
  }

  @Override
  public int getSubMeasurementIndex(String measurementId) {
    return this.measurementName.equals(measurementId) ? 0 : -1;
  }

  @Override
  public int getSubMeasurementsCount() {
    return 1;
  }

  @Override
  public boolean containsSubMeasurement(String measurementId) {
    return this.measurementName.equals(measurementId);
  }

  public void setEncoding(TSEncoding encoding) {
    this.encoding = encoding;
  }

  public void setCompressionType(CompressionType compressionType) {
    this.compressionType = compressionType;
  }
}
