/**
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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.compress.Compressor;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.encoding.encoder.TSEncodingBuilder;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.utils.StringContainer;

/**
 * This class describes a measurement's information registered in {@linkplain FileSchema FilSchema},
 * including measurement id, data type, encoding and compressor type. For each TSEncoding,
 * MeasurementSchema maintains respective TSEncodingBuilder; For TSDataType, only ENUM has
 * TSDataTypeConverter up to now.
 *
 * @author kangrong
 * @since version 0.1.0
 */
public class MeasurementSchema implements Comparable<MeasurementSchema> {

  private TSDataType type;
  private TSEncoding encoding;
  private String measurementId;
  private TSEncodingBuilder encodingConverter;
  private Compressor compressor;
  private TSFileConfig conf;
  private Map<String, String> props = new HashMap<>();

  public MeasurementSchema() {
  }

  /**
   * set properties as an empty Map.
   */
  public MeasurementSchema(String measurementId, TSDataType type, TSEncoding encoding) {
    this(measurementId, type, encoding,
        CompressionType.valueOf(TSFileDescriptor.getInstance().getConfig().compressor),
        Collections.emptyMap());
  }

  public MeasurementSchema(String measurementId, TSDataType type, TSEncoding encoding,
      CompressionType compressionType) {
    this(measurementId, type, encoding, compressionType, Collections.emptyMap());
  }

  /**
   * Constructor of MeasurementSchema.
   *
   * <p>props - information in encoding method. For RLE, Encoder.MAX_POINT_NUMBER For PLAIN,
   * Encoder.MAX_STRING_LENGTH
   */
  public MeasurementSchema(String measurementId, TSDataType type, TSEncoding encoding,
      CompressionType compressionType, Map<String, String> props) {
    this.type = type;
    this.measurementId = measurementId;
    this.encoding = encoding;
    this.props = props == null ? Collections.emptyMap() : props;
    // get config from TSFileDescriptor
    this.conf = TSFileDescriptor.getInstance().getConfig();
    // initialize TSEncoding. e.g. set max error for PLA and SDT
    encodingConverter = TSEncodingBuilder.getConverter(encoding);
    encodingConverter.initFromProps(props);
    this.compressor = Compressor.getCompressor(compressionType);
  }

  /**
   * function for deserializing data from input stream.
   */
  public static MeasurementSchema deserializeFrom(InputStream inputStream) throws IOException {
    MeasurementSchema measurementSchema = new MeasurementSchema();

    measurementSchema.measurementId = ReadWriteIOUtils.readString(inputStream);

    measurementSchema.type = ReadWriteIOUtils.readDataType(inputStream);

    measurementSchema.encoding = ReadWriteIOUtils.readEncoding(inputStream);

    CompressionType compressionType = ReadWriteIOUtils.readCompressionType(inputStream);
    measurementSchema.compressor = Compressor.getCompressor(compressionType);

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

  /**
   * function for deserializing data from byte buffer.
   */
  public static MeasurementSchema deserializeFrom(ByteBuffer buffer) {
    MeasurementSchema measurementSchema = new MeasurementSchema();

    measurementSchema.measurementId = ReadWriteIOUtils.readString(buffer);

    measurementSchema.type = ReadWriteIOUtils.readDataType(buffer);

    measurementSchema.encoding = ReadWriteIOUtils.readEncoding(buffer);

    CompressionType compressionType = ReadWriteIOUtils.readCompressionType(buffer);
    measurementSchema.compressor = Compressor.getCompressor(compressionType);

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

  public String getMeasurementId() {
    return measurementId;
  }

  public void setMeasurementId(String measurementId) {
    this.measurementId = measurementId;
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

  /**
   * return the max possible length of given type.
   *
   * @return length in unit of byte
   */
  public int getTypeLength() {
    switch (type) {
      case BOOLEAN:
        return 1;
      case INT32:
        return 4;
      case INT64:
        return 8;
      case FLOAT:
        return 4;
      case DOUBLE:
        return 8;
      case TEXT:
        // 4 is the length of string in type of Integer.
        // Note that one char corresponding to 3 byte is valid only in 16-bit BMP
        return conf.maxStringLength * TSFileConfig.BYTE_SIZE_PER_CHAR + 4;
      default:
        throw new UnSupportedDataTypeException(type.toString());
    }
  }

  /**
   * function for getting time encoder.
   */
  public Encoder getTimeEncoder() {
    TSEncoding timeSeriesEncoder = TSEncoding.valueOf(conf.timeSeriesEncoder);
    TSDataType timeType = TSDataType.valueOf(conf.timeSeriesDataType);
    return TSEncodingBuilder.getConverter(timeSeriesEncoder).getEncoder(timeType);
  }

  /**
   * get Encoder of value from encodingConverter by measurementID and data type.
   *
   * @return Encoder for value
   */
  public Encoder getValueEncoder() {
    return encodingConverter.getEncoder(type);
  }

  public Compressor getCompressor() {
    return compressor;
  }

  /**
   * function for serializing data to output stream.
   */
  public int serializeTo(OutputStream outputStream) throws IOException {
    int byteLen = 0;

    byteLen += ReadWriteIOUtils.write(measurementId, outputStream);

    byteLen += ReadWriteIOUtils.write(type, outputStream);

    byteLen += ReadWriteIOUtils.write(encoding, outputStream);

    byteLen += ReadWriteIOUtils.write(compressor.getType(), outputStream);

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

  /**
   * function for serializing data to byte buffer.
   */
  public int serializeTo(ByteBuffer buffer) {
    int byteLen = 0;

    byteLen += ReadWriteIOUtils.write(measurementId, buffer);

    byteLen += ReadWriteIOUtils.write(type, buffer);

    byteLen += ReadWriteIOUtils.write(encoding, buffer);

    byteLen += ReadWriteIOUtils.write(compressor.getType(), buffer);

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
    MeasurementSchema that = (MeasurementSchema) o;
    return type == that.type && encoding == that.encoding && Objects
        .equals(measurementId, that.measurementId)
        && Objects.equals(encodingConverter, that.encodingConverter)
        && Objects.equals(compressor, that.compressor) && Objects.equals(conf, that.conf)
        && Objects.equals(props, that.props);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, encoding, measurementId, encodingConverter, compressor, conf, props);
  }

  /**
   * compare by measurementID.
   */
  @Override
  public int compareTo(MeasurementSchema o) {
    if (equals(o)) {
      return 0;
    } else {
      return this.measurementId.compareTo(o.measurementId);
    }
  }

  @Override
  public String toString() {
    StringContainer sc = new StringContainer("");
    sc.addTail("[", measurementId, ",", type.toString(), ",", encoding.toString(), ",",
        props.toString(), ",",
        compressor.getType().toString());
    sc.addTail("]");
    return sc.toString();
  }
}
