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
package org.apache.iotdb.tsfile.write.schemaV2;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.encoding.encoder.TSEncodingBuilder;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.utils.StringContainer;

/**
 * This class describes a measurement's information registered in {@linkplain SchemaV2 FilSchema},
 * including measurement id, data type, encoding and compressor type. For each TSEncoding,
 * MeasurementSchema maintains respective TSEncodingBuilder; For TSDataType, only ENUM has
 * TSDataTypeConverter up to now.
 */
public class TimeseriesSchema implements Comparable<TimeseriesSchema>, Serializable {

  private TSDataType type;

  private TSEncoding encoding;

  private CompressionType compressionType;

  private TSEncodingBuilder encodingConverter;

  private Map<String, String> props = new HashMap<>();

  public TimeseriesSchema() {
  }

  /**
   * Constructor of MeasurementSchema.
   *
   * <p>props - information in encoding method. For RLE, Encoder.MAX_POINT_NUMBER For PLAIN,
   * Encoder.maxStringLength
   */
  public TimeseriesSchema(TSDataType type, TSEncoding encoding,
      CompressionType compressionType, Map<String, String> props) {
    this.type = type;
    this.encoding = encoding;
    this.compressionType = compressionType;
    this.props = props == null ? Collections.emptyMap() : props;
  }


  /**
   * function for deserializing data from byte buffer.
   */
  public static TimeseriesSchema deserializeFrom(ByteBuffer buffer) {
    TimeseriesSchema measurementSchema = new TimeseriesSchema();

    measurementSchema.measurementId = ReadWriteIOUtils.readString(buffer);

    measurementSchema.type = ReadWriteIOUtils.readDataType(buffer);

    measurementSchema.encoding = ReadWriteIOUtils.readEncoding(buffer);

    measurementSchema.compressionType = ReadWriteIOUtils.readCompressionType(buffer);

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

  public String getDeviceId() {
    return deviceId;
  }

  public void setDeviceId(String deviceId) {
    this.deviceId = deviceId;
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

  public void setProps(Map<String, String> props) {
    this.props = props;
  }

  /**

  /**
   * function for getting time encoder.
   * TODO can I be optimized?
   */
  public Encoder getTimeEncoder() {
    TSEncoding timeSeriesEncoder = TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder());
    TSDataType timeType = TSDataType.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeSeriesDataType());
    return TSEncodingBuilder.getConverter(timeSeriesEncoder).getEncoder(timeType);
  }

  /**
   * get Encoder of value from encodingConverter by measurementID and data type.
   * TODO can I be optimized?
   * @return Encoder for value
   */
  public Encoder getValueEncoder() {
    //it is ok even if encodingConverter is constructed two instances for concurrent scenario..
    if (encodingConverter == null) {
      // initialize TSEncoding. e.g. set max error for PLA and SDT
      encodingConverter = TSEncodingBuilder.getConverter(encoding);
      encodingConverter.initFromProps(props);
    }
    return encodingConverter.getEncoder(type);
  }

  public CompressionType getCompressionType() {
    return compressionType;
  }

}
