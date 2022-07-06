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

package org.apache.iotdb.db.mpp.common.schematree.node;

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class SchemaMeasurementNode extends SchemaNode {

  private String alias;
  private MeasurementSchema schema;

  public SchemaMeasurementNode(String name, MeasurementSchema schema) {
    super(name);
    this.schema = schema;
  }

  public String getAlias() {
    return alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  public MeasurementSchema getSchema() {
    return schema;
  }

  @Override
  public void replaceChild(String name, SchemaNode newChild) {
    throw new UnsupportedOperationException(
        "This operation is not supported in SchemaMeasurementNode.");
  }

  @Override
  public void copyDataTo(SchemaNode schemaNode) {
    if (!schemaNode.isMeasurement()) {
      return;
    }
    SchemaMeasurementNode measurementNode = schemaNode.getAsMeasurementNode();
    measurementNode.setSchema(schema);
    measurementNode.setAlias(alias);
  }

  private void setSchema(MeasurementSchema schema) {
    this.schema = schema;
  }

  @Override
  public boolean isMeasurement() {
    return true;
  }

  @Override
  public SchemaMeasurementNode getAsMeasurementNode() {
    return this;
  }

  @Override
  public byte getType() {
    return SCHEMA_MEASUREMENT_NODE;
  }

  @Override
  public void serialize(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(getType(), outputStream);
    ReadWriteIOUtils.write(name, outputStream);

    ReadWriteIOUtils.write(alias, outputStream);
    schema.serializeTo(outputStream);
  }

  public static SchemaMeasurementNode deserialize(InputStream inputStream) throws IOException {
    String name = ReadWriteIOUtils.readString(inputStream);
    String alias = ReadWriteIOUtils.readString(inputStream);
    MeasurementSchema schema = MeasurementSchema.deserializeFrom(inputStream);

    SchemaMeasurementNode measurementNode = new SchemaMeasurementNode(name, schema);
    measurementNode.setAlias(alias);
    return measurementNode;
  }
}
