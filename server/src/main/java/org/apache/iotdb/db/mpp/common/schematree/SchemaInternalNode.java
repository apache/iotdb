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
import java.util.Iterator;
import java.util.Map;

public class SchemaInternalNode extends SchemaNode {

  protected Map<String, SchemaNode> children;

  public SchemaInternalNode(String name) {
    super(name);
  }

  @Override
  public SchemaNode getChild(String name) {
    return children == null ? null : children.get(name);
  }

  public void addChild(String name, SchemaNode child) {
    if (children == null) {
      children = new HashMap<>();
    }
    children.put(name, child);
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

  public void serialize(ByteBuffer buffer) {
    serializeChildren(buffer);

    ReadWriteIOUtils.write(getType(), buffer);
    ReadWriteIOUtils.write(name, buffer);
    ReadWriteIOUtils.write(children.size(), buffer);
  }

  protected void serializeChildren(ByteBuffer buffer) {
    for (SchemaNode child : children.values()) {
      child.serialize(buffer);
    }
  }

  public static SchemaInternalNode deserialize(ByteBuffer buffer) {
    String name = ReadWriteIOUtils.readString(buffer);

    return new SchemaInternalNode(name);
  }
}
