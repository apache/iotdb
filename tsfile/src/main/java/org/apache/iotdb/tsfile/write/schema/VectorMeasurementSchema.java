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
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class VectorMeasurementSchema
    implements IMeasurementSchema, Comparable<VectorMeasurementSchema>, Serializable {

  public static final String VECTOR_NAME_PREFIX = "$#$";

  // this is equal to the time id in this vector
  private String vectorMeausurementId;
  private String[] measurements;
  private byte[] types;
  private byte[] encodings;
  private TSEncodingBuilder[] encodingConverters;
  private byte compressor;

  public VectorMeasurementSchema() {}

  public VectorMeasurementSchema(
      String measurementId,
      String[] measurements,
      TSDataType[] types,
      TSEncoding[] encodings,
      CompressionType compressionType) {
    this.vectorMeausurementId = measurementId;
    this.measurements = measurements;
    byte[] typesInByte = new byte[types.length];
    for (int i = 0; i < types.length; i++) {
      typesInByte[i] = types[i].serialize();
    }
    this.types = typesInByte;

    byte[] encodingsInByte = new byte[encodings.length];
    for (int i = 0; i < encodings.length; i++) {
      encodingsInByte[i] = encodings[i].serialize();
    }
    this.encodings = encodingsInByte;
    this.encodingConverters = new TSEncodingBuilder[measurements.length];
    this.compressor = compressionType.serialize();
  }

  public VectorMeasurementSchema(
      String[] measurements, byte[] types, byte[] encodings, byte compressor) {
    this.measurements = measurements;
    this.types = types;
    this.encodings = encodings;
    this.encodingConverters = new TSEncodingBuilder[measurements.length];
    this.compressor = compressor;
  }

  public VectorMeasurementSchema(String[] measurements, TSDataType[] types) {
    this.measurements = measurements;
    this.types = new byte[types.length];
    for (int i = 0; i < types.length; i++) {
      this.types[i] = types[i].serialize();
    }

    this.encodings = new byte[types.length];
    for (int i = 0; i < types.length; i++) {
      this.encodings[i] =
          TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getValueEncoder())
              .serialize();
    }
    this.encodingConverters = new TSEncodingBuilder[measurements.length];
    this.compressor = TSFileDescriptor.getInstance().getConfig().getCompressor().serialize();
  }

  public VectorMeasurementSchema(
      String measurementId, String[] measurements, TSDataType[] types, TSEncoding[] encodings) {
    this(
        measurementId,
        measurements,
        types,
        encodings,
        TSFileDescriptor.getInstance().getConfig().getCompressor());
  }

  @Override
  public String getMeasurementId() {
    return vectorMeausurementId;
  }

  @Override
  public CompressionType getCompressor() {
    return CompressionType.deserialize(compressor);
  }

  @Override
  public TSEncoding getEncodingType() {
    throw new UnsupportedOperationException("unsupported method for VectorMeasurementSchema");
  }

  @Override
  public TSDataType getType() {
    return TSDataType.VECTOR;
  }

  @Override
  public void setType(TSDataType dataType) {
    throw new UnsupportedOperationException("unsupported method for VectorMeasurementSchema");
  }

  @Override
  public TSEncoding getTimeTSEncoding() {
    return TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder());
  }

  @Override
  public Encoder getTimeEncoder() {
    TSEncoding timeEncoding =
        TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder());
    TSDataType timeType =
        TSDataType.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeSeriesDataType());
    return TSEncodingBuilder.getEncodingBuilder(timeEncoding).getEncoder(timeType);
  }

  @Override
  public Encoder getValueEncoder() {
    throw new UnsupportedOperationException("unsupported method for VectorMeasurementSchema");
  }

  @Override
  public Map<String, String> getProps() {
    throw new UnsupportedOperationException("unsupported method for VectorMeasurementSchema");
  }

  @Override
  public List<String> getValueMeasurementIdList() {
    return Arrays.asList(measurements);
  }

  @Override
  public List<TSDataType> getValueTSDataTypeList() {
    List<TSDataType> dataTypeList = new ArrayList<>();
    for (byte dataType : types) {
      dataTypeList.add(TSDataType.deserialize(dataType));
    }
    return dataTypeList;
  }

  @Override
  public List<TSEncoding> getValueTSEncodingList() {
    List<TSEncoding> encodingList = new ArrayList<>();
    for (byte encoding : encodings) {
      encodingList.add(TSEncoding.deserialize(encoding));
    }
    return encodingList;
  }

  @Override
  public List<Encoder> getValueEncoderList() {
    List<Encoder> encoderList = new ArrayList<>();
    for (int i = 0; i < encodings.length; i++) {
      TSEncoding encoding = TSEncoding.deserialize(encodings[i]);
      // it is ok even if encodingConverter is constructed two instances for concurrent scenario
      if (encodingConverters[i] == null) {
        // initialize TSEncoding. e.g. set max error for PLA and SDT
        encodingConverters[i] = TSEncodingBuilder.getEncodingBuilder(encoding);
        encodingConverters[i].initFromProps(null);
      }
      encoderList.add(encodingConverters[i].getEncoder(TSDataType.deserialize(types[i])));
    }
    return encoderList;
  }

  @Override
  public int getMeasurementIdColumnIndex(String measurementId) {
    return getValueMeasurementIdList().indexOf(measurementId);
  }

  @Override
  public int serializeTo(ByteBuffer buffer) {
    int byteLen = 0;
    byteLen +=
        ReadWriteIOUtils.write(vectorMeausurementId.substring(VECTOR_NAME_PREFIX.length()), buffer);
    byteLen += ReadWriteIOUtils.write(measurements.length, buffer);

    for (String measurementId : measurements) {
      byteLen += ReadWriteIOUtils.write(measurementId, buffer);
    }
    for (byte type : types) {
      byteLen += ReadWriteIOUtils.write(type, buffer);
    }
    for (byte encoding : encodings) {
      byteLen += ReadWriteIOUtils.write(encoding, buffer);
    }
    byteLen += ReadWriteIOUtils.write(compressor, buffer);

    return byteLen;
  }

  @Override
  public int serializeTo(OutputStream outputStream) throws IOException {
    int byteLen = 0;
    byteLen +=
        ReadWriteIOUtils.write(
            vectorMeausurementId.substring(VECTOR_NAME_PREFIX.length()), outputStream);
    byteLen += ReadWriteIOUtils.write(measurements.length, outputStream);

    for (String measurementId : measurements) {
      byteLen += ReadWriteIOUtils.write(measurementId, outputStream);
    }
    for (byte type : types) {
      byteLen += ReadWriteIOUtils.write(type, outputStream);
    }
    for (byte encoding : encodings) {
      byteLen += ReadWriteIOUtils.write(encoding, outputStream);
    }
    byteLen += ReadWriteIOUtils.write(compressor, outputStream);

    return byteLen;
  }

  @Override
  public int partialSerializeTo(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write((byte) 1, outputStream);
    return 1 + serializeTo(outputStream);
  }

  @Override
  public int partialSerializeTo(ByteBuffer buffer) {
    ReadWriteIOUtils.write((byte) 1, buffer);
    return 1 + serializeTo(buffer);
  }

  public static VectorMeasurementSchema partialDeserializeFrom(ByteBuffer buffer) {
    return deserializeFrom(buffer);
  }

  public static VectorMeasurementSchema deserializeFrom(InputStream inputStream)
      throws IOException {
    VectorMeasurementSchema vectorMeasurementSchema = new VectorMeasurementSchema();
    vectorMeasurementSchema.vectorMeausurementId =
        VECTOR_NAME_PREFIX + ReadWriteIOUtils.readString(inputStream);

    int measurementSize = ReadWriteIOUtils.readInt(inputStream);
    String[] measurements = new String[measurementSize];
    for (int i = 0; i < measurementSize; i++) {
      measurements[i] = ReadWriteIOUtils.readString(inputStream);
    }
    vectorMeasurementSchema.measurements = measurements;

    byte[] types = new byte[measurementSize];
    for (int i = 0; i < measurementSize; i++) {
      types[i] = ReadWriteIOUtils.readByte(inputStream);
    }
    vectorMeasurementSchema.types = types;

    byte[] encodings = new byte[measurementSize];
    for (int i = 0; i < measurementSize; i++) {
      encodings[i] = ReadWriteIOUtils.readByte(inputStream);
    }
    vectorMeasurementSchema.encodings = encodings;

    vectorMeasurementSchema.compressor = ReadWriteIOUtils.readByte(inputStream);
    return vectorMeasurementSchema;
  }

  public static VectorMeasurementSchema deserializeFrom(ByteBuffer buffer) {
    VectorMeasurementSchema vectorMeasurementSchema = new VectorMeasurementSchema();
    vectorMeasurementSchema.vectorMeausurementId =
        VECTOR_NAME_PREFIX + ReadWriteIOUtils.readString(buffer);
    int measurementSize = ReadWriteIOUtils.readInt(buffer);
    String[] measurements = new String[measurementSize];
    for (int i = 0; i < measurementSize; i++) {
      measurements[i] = ReadWriteIOUtils.readString(buffer);
    }
    vectorMeasurementSchema.measurements = measurements;

    byte[] types = new byte[measurementSize];
    for (int i = 0; i < measurementSize; i++) {
      types[i] = ReadWriteIOUtils.readByte(buffer);
    }
    vectorMeasurementSchema.types = types;

    byte[] encodings = new byte[measurementSize];
    for (int i = 0; i < measurementSize; i++) {
      encodings[i] = ReadWriteIOUtils.readByte(buffer);
    }
    vectorMeasurementSchema.encodings = encodings;

    vectorMeasurementSchema.compressor = ReadWriteIOUtils.readByte(buffer);
    return vectorMeasurementSchema;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    VectorMeasurementSchema that = (VectorMeasurementSchema) o;
    return Arrays.equals(types, that.types)
        && Arrays.equals(encodings, that.encodings)
        && Arrays.equals(measurements, that.measurements)
        && Objects.equals(compressor, that.compressor);
  }

  @Override
  public int hashCode() {
    return Objects.hash(types, encodings, measurements, compressor);
  }

  /** compare by first measurementID. */
  @Override
  public int compareTo(VectorMeasurementSchema o) {
    if (equals(o)) {
      return 0;
    } else {
      return this.measurements[0].compareTo(o.measurements[0]);
    }
  }

  @Override
  public String toString() {
    StringContainer sc = new StringContainer("");
    for (int i = 0; i < measurements.length; i++) {
      sc.addTail(
          "[",
          measurements[i],
          ",",
          TSDataType.deserialize(types[i]).toString(),
          ",",
          TSEncoding.deserialize(encodings[i]).toString());
      sc.addTail("],");
    }
    sc.addTail(CompressionType.deserialize(compressor).toString());
    return sc.toString();
  }
}
