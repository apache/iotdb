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
package org.apache.iotdb.db.metadata.template;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Template implements Serializable {

  private int id;
  private String name;
  private boolean isDirectAligned;
  private Map<String, IMeasurementSchema> schemaMap;

  private transient int rehashCode;

  public Template() {
    schemaMap = new HashMap<>();
  }

  public Template(
      String name,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors)
      throws IllegalPathException {
    this(name, measurements, dataTypes, encodings, compressors, false);
  }

  public Template(
      String name,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      boolean isAligned)
      throws IllegalPathException {
    this.isDirectAligned = isAligned;
    this.schemaMap = new HashMap<>();
    this.name = name;
    for (int i = 0; i < measurements.size(); i++) {
      IMeasurementSchema schema =
          new MeasurementSchema(
              measurements.get(i), dataTypes.get(i), encodings.get(i), compressors.get(i));
      schemaMap.put(schema.getMeasurementId(), schema);
    }
  }

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Map<String, IMeasurementSchema> getSchemaMap() {
    return schemaMap;
  }

  public boolean hasSchema(String suffixPath) {
    return schemaMap.containsKey(suffixPath);
  }

  public IMeasurementSchema getSchema(String measurementId) {
    return schemaMap.get(measurementId);
  }

  public boolean isDirectAligned() {
    return isDirectAligned;
  }

  // region construct template tree

  private IMeasurementSchema constructSchema(
      String nodeName, TSDataType dataType, TSEncoding encoding, CompressionType compressor) {
    return new MeasurementSchema(nodeName, dataType, encoding, compressor);
  }

  // endregion

  // region append of template

  public void addMeasurements(
      String[] measurements,
      TSDataType[] dataTypes,
      TSEncoding[] encodings,
      CompressionType[] compressors)
      throws IllegalPathException {
    // check exists
    for (String measurement : measurements) {
      if (schemaMap.containsKey(measurement)) {
        throw new IllegalPathException(measurement, "path already exists");
      }
    }
    // construct
    for (int i = 0; i < measurements.length; i++) {
      IMeasurementSchema schema =
          constructSchema(measurements[i], dataTypes[i], encodings[i], compressors[i]);
      schemaMap.put(measurements[i], schema);
    }
  }

  // endregion

  public void serialize(ByteBuffer buffer) {
    ReadWriteIOUtils.write(id, buffer);
    ReadWriteIOUtils.write(name, buffer);
    ReadWriteIOUtils.write(isDirectAligned, buffer);
    ReadWriteIOUtils.write(schemaMap.size(), buffer);
    for (Map.Entry<String, IMeasurementSchema> entry : schemaMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), buffer);
      entry.getValue().partialSerializeTo(buffer);
    }
  }

  public void serialize(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(id, outputStream);
    ReadWriteIOUtils.write(name, outputStream);
    ReadWriteIOUtils.write(isDirectAligned, outputStream);
    ReadWriteIOUtils.write(schemaMap.size(), outputStream);
    for (Map.Entry<String, IMeasurementSchema> entry : schemaMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      entry.getValue().partialSerializeTo(outputStream);
    }
  }

  public ByteBuffer serialize() {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    try {
      serialize(outputStream);
    } catch (IOException ignored) {

    }
    return ByteBuffer.wrap(outputStream.toByteArray());
  }

  public void deserialize(ByteBuffer buffer) {
    id = ReadWriteIOUtils.readInt(buffer);
    name = ReadWriteIOUtils.readString(buffer);
    isDirectAligned = ReadWriteIOUtils.readBool(buffer);
    int schemaSize = ReadWriteIOUtils.readInt(buffer);
    schemaMap = new HashMap<>(schemaSize);
    for (int i = 0; i < schemaSize; i++) {
      String schemaName = ReadWriteIOUtils.readString(buffer);
      byte flag = ReadWriteIOUtils.readByte(buffer);
      IMeasurementSchema measurementSchema = null;
      if (flag == (byte) 0) {
        measurementSchema = MeasurementSchema.partialDeserializeFrom(buffer);
      } else if (flag == (byte) 1) {
        measurementSchema = VectorMeasurementSchema.partialDeserializeFrom(buffer);
      }
      schemaMap.put(schemaName, measurementSchema);
    }
  }

  @Override
  public boolean equals(Object t) {
    if (this == t) {
      return true;
    }
    if (t == null || getClass() != t.getClass()) {
      return false;
    }
    Template that = (Template) t;
    return this.name.equals(that.name) && this.schemaMap.equals(that.schemaMap);
  }

  @Override
  public int hashCode() {
    return rehashCode != 0
        ? rehashCode
        : new HashCodeBuilder(17, 37).append(name).append(schemaMap).toHashCode();
  }
}
