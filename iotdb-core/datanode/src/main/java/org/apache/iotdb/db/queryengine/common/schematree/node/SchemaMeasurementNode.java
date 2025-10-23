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

package org.apache.iotdb.db.queryengine.common.schematree.node;

import org.apache.iotdb.commons.schema.view.LogicalViewSchema;
import org.apache.iotdb.db.queryengine.common.schematree.IMeasurementSchemaInfo;

import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchemaType;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

public class SchemaMeasurementNode extends SchemaNode implements IMeasurementSchemaInfo {

  private static final long SHALLOW_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(SchemaMeasurementNode.class);

  private String alias;
  private IMeasurementSchema schema;
  private Map<String, String> tagMap;
  private Map<String, String> attributeMap;

  public SchemaMeasurementNode(String name, IMeasurementSchema schema) {
    super(name);
    this.schema = schema;
  }

  @Override
  public long ramBytesUsed() {
    return SHALLOW_SIZE
        + RamUsageEstimator.sizeOf(name)
        + RamUsageEstimator.sizeOf(alias)
        + schema.ramBytesUsed()
        + RamUsageEstimator.sizeOfMapWithKnownShallowSize(
            tagMap,
            RamUsageEstimator.SHALLOW_SIZE_OF_HASHMAP,
            RamUsageEstimator.SHALLOW_SIZE_OF_HASHMAP_ENTRY)
        + RamUsageEstimator.sizeOfMapWithKnownShallowSize(
            attributeMap,
            RamUsageEstimator.SHALLOW_SIZE_OF_HASHMAP,
            RamUsageEstimator.SHALLOW_SIZE_OF_HASHMAP_ENTRY);
  }

  public String getAlias() {
    return alias;
  }

  @Override
  public boolean isLogicalView() {
    return this.schema.isLogicalView();
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  public IMeasurementSchema getSchema() {
    return schema;
  }

  @Override
  public MeasurementSchema getSchemaAsMeasurementSchema() {
    if (this.schema instanceof MeasurementSchema) {
      return (MeasurementSchema) this.getSchema();
    }
    return null;
  }

  @Override
  public LogicalViewSchema getSchemaAsLogicalViewSchema() {
    if (this.schema instanceof LogicalViewSchema) {
      return (LogicalViewSchema) this.getSchema();
    }
    return null;
  }

  @Override
  public Map<String, String> getTagMap() {
    return tagMap;
  }

  @Override
  public Map<String, String> getAttributeMap() {
    return attributeMap;
  }

  public void setAttributeMap(Map<String, String> attributeMap) {
    this.attributeMap = attributeMap;
  }

  @Override
  public SchemaNode getChild(String name) {
    return null;
  }

  @Override
  public void replaceChild(String name, SchemaNode newChild) {
    throw new UnsupportedOperationException(
        "This operation is not supported in SchemaMeasurementNode.");
  }

  @Override
  public void removeChild(String name) {
    throw new UnsupportedOperationException(
        "Remove child operation is not supported in SchemaMeasurementNode.");
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

  private void setSchema(IMeasurementSchema schema) {
    this.schema = schema;
  }

  public void setTagMap(Map<String, String> tagMap) {
    this.tagMap = tagMap;
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
    serializeNodeOwnContent(outputStream);
  }

  public void serializeNodeOwnContent(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(getType(), outputStream);
    ReadWriteIOUtils.write(name, outputStream);
    ReadWriteIOUtils.write(alias, outputStream);

    MeasurementSchemaType measurementSchemaType = schema.getSchemaType();
    ReadWriteIOUtils.write(
        measurementSchemaType.getMeasurementSchemaTypeInByteEnum(), outputStream);
    schema.serializeTo(outputStream);

    ReadWriteIOUtils.write(tagMap, outputStream);
    ReadWriteIOUtils.write(attributeMap, outputStream);
  }

  public static SchemaMeasurementNode deserialize(InputStream inputStream) throws IOException {
    String name = ReadWriteIOUtils.readString(inputStream);
    String alias = ReadWriteIOUtils.readString(inputStream);

    IMeasurementSchema schema = null;
    byte measurementSchemaType = ReadWriteIOUtils.readByte(inputStream);
    if (measurementSchemaType
        == MeasurementSchemaType.MEASUREMENT_SCHEMA.getMeasurementSchemaTypeInByteEnum()) {
      schema = MeasurementSchema.deserializeFrom(inputStream);
    } else if (measurementSchemaType
        == MeasurementSchemaType.LOGICAL_VIEW_SCHEMA.getMeasurementSchemaTypeInByteEnum()) {
      schema = LogicalViewSchema.deserializeFrom(inputStream);
    }

    Map<String, String> tagMap = ReadWriteIOUtils.readMap(inputStream);
    Map<String, String> attributeMap = ReadWriteIOUtils.readMap(inputStream);

    SchemaMeasurementNode measurementNode = new SchemaMeasurementNode(name, schema);
    measurementNode.setAlias(alias);
    measurementNode.setTagMap(tagMap);
    measurementNode.setAttributeMap(attributeMap);
    return measurementNode;
  }
}
