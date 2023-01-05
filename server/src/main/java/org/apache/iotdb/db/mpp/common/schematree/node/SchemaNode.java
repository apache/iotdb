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

import org.apache.iotdb.commons.schema.tree.ITreeNode;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

public abstract class SchemaNode implements ITreeNode {

  public static final byte SCHEMA_INTERNAL_NODE = 0;
  public static final byte SCHEMA_ENTITY_NODE = 1;
  public static final byte SCHEMA_MEASUREMENT_NODE = 2;

  protected final String name;

  protected SchemaNode(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public SchemaNode getChild(String name) {
    return null;
  }

  public void addChild(String name, SchemaNode child) {}

  public abstract void replaceChild(String name, SchemaNode newChild);

  public abstract void copyDataTo(SchemaNode schemaNode);

  public Map<String, SchemaNode> getChildren() {
    return Collections.emptyMap();
  }

  public Iterator<SchemaNode> getChildrenIterator() {
    return Collections.emptyIterator();
  }

  public boolean isEntity() {
    return false;
  }

  public boolean isMeasurement() {
    return false;
  }

  public SchemaEntityNode getAsEntityNode() {
    throw new UnsupportedOperationException("This node isn't instance of SchemaEntityNode.");
  }

  public SchemaMeasurementNode getAsMeasurementNode() {
    throw new UnsupportedOperationException("This node isn't instance of SchemaMeasurementNode.");
  }

  public abstract byte getType();

  public abstract void serialize(OutputStream outputStream) throws IOException;
}
