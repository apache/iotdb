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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

public class SchemaEntityNode extends SchemaInternalNode {

  private boolean isAligned;

  private Map<String, SchemaMeasurementNode> aliasChildren;

  public SchemaEntityNode(String name) {
    super(name);
  }

  @Override
  public SchemaNode getChild(String name) {
    SchemaNode node = super.getChild(name);
    if (node != null) {
      return node;
    }
    return aliasChildren == null ? null : aliasChildren.get(name);
  }

  public void addAliasChild(String alias, SchemaMeasurementNode measurementNode) {
    if (aliasChildren == null) {
      aliasChildren = new HashMap<>();
    }
    aliasChildren.put(alias, measurementNode);
  }

  public boolean isAligned() {
    return isAligned;
  }

  public void setAligned(boolean aligned) {
    isAligned = aligned;
  }

  @Override
  public void replaceChild(String name, SchemaNode newChild) {
    super.replaceChild(name, newChild);
    if (newChild.isMeasurement()) {
      SchemaMeasurementNode measurementNode = newChild.getAsMeasurementNode();
      if (measurementNode.getAlias() != null) {
        aliasChildren.replace(name, measurementNode);
      }
    }
  }

  @Override
  public void copyDataTo(SchemaNode schemaNode) {
    if (!schemaNode.isEntity()) {
      return;
    }
    SchemaEntityNode entityNode = schemaNode.getAsEntityNode();
    entityNode.setAligned(isAligned);
    if (aliasChildren != null) {
      for (SchemaMeasurementNode child : aliasChildren.values()) {
        entityNode.addAliasChild(child.getAlias(), child);
      }
    }
  }

  @Override
  public boolean isEntity() {
    return true;
  }

  @Override
  public SchemaEntityNode getAsEntityNode() {
    return this;
  }

  @Override
  public byte getType() {
    return SCHEMA_ENTITY_NODE;
  }

  @Override
  public void serialize(OutputStream outputStream) throws IOException {
    serializeChildren(outputStream);

    ReadWriteIOUtils.write(getType(), outputStream);
    ReadWriteIOUtils.write(name, outputStream);
    ReadWriteIOUtils.write(isAligned, outputStream);
    ReadWriteIOUtils.write(children.size(), outputStream);
  }

  public static SchemaEntityNode deserialize(InputStream inputStream) throws IOException {
    String name = ReadWriteIOUtils.readString(inputStream);
    boolean isAligned = ReadWriteIOUtils.readBool(inputStream);

    SchemaEntityNode entityNode = new SchemaEntityNode(name);
    entityNode.setAligned(isAligned);
    return entityNode;
  }
}
