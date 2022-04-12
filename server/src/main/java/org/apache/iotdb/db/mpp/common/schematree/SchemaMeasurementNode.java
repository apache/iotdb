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

package org.apache.iotdb.db.mpp.common.schematree;

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.nio.ByteBuffer;

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
  public void serialize(ByteBuffer buffer) {
    ReadWriteIOUtils.write(getType(), buffer);
    ReadWriteIOUtils.write(name, buffer);

    ReadWriteIOUtils.write(alias, buffer);
    schema.serializeTo(buffer);
  }

  public static SchemaMeasurementNode deserialize(ByteBuffer buffer) {
    String name = ReadWriteIOUtils.readString(buffer);
    String alias = ReadWriteIOUtils.readString(buffer);
    MeasurementSchema schema = MeasurementSchema.deserializeFrom(buffer);

    SchemaMeasurementNode measurementNode = new SchemaMeasurementNode(name, schema);
    measurementNode.setAlias(alias);
    return measurementNode;
  }
}
