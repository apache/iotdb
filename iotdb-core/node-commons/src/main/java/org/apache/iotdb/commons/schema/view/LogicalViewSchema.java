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

package org.apache.iotdb.commons.schema.view;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.view.viewExpression.ViewExpression;
import org.apache.iotdb.commons.schema.view.viewExpression.leaf.TimeSeriesViewOperand;

import org.apache.tsfile.encoding.encoder.Encoder;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchemaType;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LogicalViewSchema
    implements IMeasurementSchema, Comparable<LogicalViewSchema>, Serializable {

  private String measurementId;

  private ViewExpression expression;

  private TSDataType dataType = TSDataType.UNKNOWN;

  public LogicalViewSchema(String measurementId, ViewExpression expression) {
    this.measurementId = measurementId;
    this.expression = expression;
  }

  @Override
  public int compareTo(LogicalViewSchema o) {
    if (equals(o)) {
      return 0;
    } else {
      return this.measurementId.compareTo(o.measurementId);
    }
  }

  @Override
  public MeasurementSchemaType getSchemaType() {
    return MeasurementSchemaType.LOGICAL_VIEW_SCHEMA;
  }

  @Override
  public String getMeasurementId() {
    return this.measurementId;
  }

  @Override
  public CompressionType getCompressor() {
    // TODO: CRTODO:add new CompressionType
    return CompressionType.UNCOMPRESSED;
  }

  @Override
  public TSEncoding getEncodingType() {
    // TODO: CRTODO: add new TSEncoding
    return TSEncoding.PLAIN;
  }

  @Override
  public TSDataType getType() {
    return dataType;
  }

  @Override
  public byte getTypeInByte() {
    return TSDataType.BOOLEAN.getType();
  }

  @Override
  public void setType(TSDataType dataType) {
    this.dataType = dataType;
  }

  @Override
  public TSEncoding getTimeTSEncoding() {
    // TODO: CRTODO: add new TSEncoding
    return TSEncoding.PLAIN;
  }

  @Override
  public Encoder getTimeEncoder() {
    // TODO: CRTODO: is this ok to return a null value?
    return null;
  }

  @Override
  public Encoder getValueEncoder() {
    // TODO: CRTODO: is this ok to return a null value?
    return null;
  }

  @Override
  public Map<String, String> getProps() {
    return new HashMap<>();
  }

  @Override
  public List<String> getSubMeasurementsList() {
    throw new UnsupportedOperationException("unsupported method for LogicalViewSchema");
  }

  @Override
  public List<TSDataType> getSubMeasurementsTSDataTypeList() {
    throw new UnsupportedOperationException("unsupported method for LogicalViewSchema");
  }

  @Override
  public List<TSEncoding> getSubMeasurementsTSEncodingList() {
    throw new UnsupportedOperationException("unsupported method for LogicalViewSchema");
  }

  @Override
  public List<Encoder> getSubMeasurementsEncoderList() {
    throw new UnsupportedOperationException("unsupported method for LogicalViewSchema");
  }

  @Override
  public int getSubMeasurementIndex(String measurementId) {
    return this.measurementId.equals(measurementId) ? 0 : -1;
  }

  @Override
  public int getSubMeasurementsCount() {
    return 1;
  }

  @Override
  public boolean containsSubMeasurement(String measurementId) {
    return this.measurementId.equals(measurementId);
  }

  // region serialize and deserialize

  @Override
  public int serializedSize() {
    throw new RuntimeException(
        new UnsupportedOperationException(
            "Can not calculate the size of view schemaengine before serializing."));
  }

  @Override
  public int serializeTo(ByteBuffer buffer) {
    // TODO: CRTODO: the size of buffer is not calculated!
    ReadWriteIOUtils.write(measurementId, buffer);

    ViewExpression.serialize(this.expression, buffer);
    return 0;
  }

  @Override
  public int serializeTo(OutputStream outputStream) throws IOException {
    // TODO: CRTODO: the size of buffer is not calculated!
    ReadWriteIOUtils.write(measurementId, outputStream);

    ViewExpression.serialize(this.expression, outputStream);
    return 0;
  }

  @Override
  public int partialSerializeTo(ByteBuffer buffer) {
    return this.serializeTo(buffer);
  }

  @Override
  public int partialSerializeTo(OutputStream outputStream) throws IOException {
    return this.serializeTo(outputStream);
  }

  @Override
  public boolean isLogicalView() {
    return true;
  }

  public static LogicalViewSchema deserializeFrom(InputStream inputStream) throws IOException {
    String measurementId = ReadWriteIOUtils.readString(inputStream);

    ViewExpression expression = ViewExpression.deserialize(inputStream);

    return new LogicalViewSchema(measurementId, expression);
  }

  public static LogicalViewSchema deserializeFrom(ByteBuffer buffer) {
    String measurementId = ReadWriteIOUtils.readString(buffer);

    ViewExpression expression = ViewExpression.deserialize(buffer);

    return new LogicalViewSchema(measurementId, expression);
  }

  // endregion

  public ViewExpression getExpression() {
    return this.expression;
  }

  public void setExpression(ViewExpression expression) {
    this.expression = expression;
  }

  public boolean isWritable() {
    return this.expression instanceof TimeSeriesViewOperand;
  }

  public String getSourcePathStringIfWritable() {
    if (this.isWritable()) {
      return ((TimeSeriesViewOperand) this.expression).getPathString();
    }
    return null;
  }

  public PartialPath getSourcePathIfWritable() {
    if (this.isWritable()) {
      try {
        return new MeasurementPath(((TimeSeriesViewOperand) this.expression).getPathString());
      } catch (IllegalPathException e) {
        throw new RuntimeException(
            new MetadataException(
                String.format(
                    "View with measurementID [%s] is broken. It stores illegal path [%s].",
                    this.measurementId, this.getSourcePathStringIfWritable())));
      }
    }
    return null;
  }
}
