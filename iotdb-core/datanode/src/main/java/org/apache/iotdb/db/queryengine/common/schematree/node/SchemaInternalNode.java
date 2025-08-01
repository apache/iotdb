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

import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class SchemaInternalNode extends SchemaNode {

  private static final long SHALLOW_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(SchemaInternalNode.class);

  protected Map<String, SchemaNode> children = new HashMap<>();

  public SchemaInternalNode(String name) {
    super(name);
  }

  @Override
  public SchemaNode getChild(String name) {
    return children.get(name);
  }

  @Override
  public void addChild(String name, SchemaNode child) {
    children.put(name, child);
  }

  @Override
  public void replaceChild(String name, SchemaNode newChild) {
    SchemaNode oldChild = children.get(name);
    oldChild.copyDataTo(newChild);
    children.replace(name, newChild);
  }

  @Override
  public void removeChild(String name) {
    children.remove(name);
  }

  @Override
  public void copyDataTo(SchemaNode schemaNode) {
    if (schemaNode.isMeasurement()) {
      return;
    }
    for (SchemaNode child : children.values()) {
      schemaNode.addChild(child.getName(), child);
    }
  }

  @Override
  public Map<String, SchemaNode> getChildren() {
    return children;
  }

  @Override
  public Iterator<SchemaNode> getChildrenIterator() {
    return children.values().iterator();
  }

  @Override
  public byte getType() {
    return SCHEMA_INTERNAL_NODE;
  }

  public void serialize(OutputStream outputStream) throws IOException {
    serializeChildren(outputStream);
    serializeNodeOwnContent(outputStream);
  }

  public void serializeNodeOwnContent(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(getType(), outputStream);
    ReadWriteIOUtils.write(name, outputStream);
    ReadWriteIOUtils.write(children.size(), outputStream);
  }

  protected void serializeChildren(OutputStream outputStream) throws IOException {
    for (SchemaNode child : children.values()) {
      child.serialize(outputStream);
    }
  }

  public static SchemaInternalNode deserialize(InputStream inputStream) throws IOException {
    String name = ReadWriteIOUtils.readString(inputStream);

    return new SchemaInternalNode(name);
  }

  @Override
  public long ramBytesUsed() {
    return SHALLOW_SIZE
        + RamUsageEstimator.sizeOf(name)
        + RamUsageEstimator.sizeOfMapWithKnownShallowSize(
            children,
            RamUsageEstimator.SHALLOW_SIZE_OF_HASHMAP,
            RamUsageEstimator.SHALLOW_SIZE_OF_HASHMAP_ENTRY);
  }
}
