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

import java.nio.ByteBuffer;
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
  public void serialize(ByteBuffer buffer) {
    serializeChildren(buffer);

    ReadWriteIOUtils.write(getType(), buffer);
    ReadWriteIOUtils.write(name, buffer);
    ReadWriteIOUtils.write(isAligned, buffer);
    ReadWriteIOUtils.write(children.size(), buffer);
  }

  public static SchemaEntityNode deserialize(ByteBuffer buffer) {
    String name = ReadWriteIOUtils.readString(buffer);
    boolean isAligned = ReadWriteIOUtils.readBool(buffer);

    SchemaEntityNode entityNode = new SchemaEntityNode(name);
    entityNode.setAligned(isAligned);
    return entityNode;
  }
}
