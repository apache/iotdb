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

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.common.schematree.node.SchemaNode;
import org.apache.iotdb.db.mpp.common.schematree.visitor.SchemaTreeMeasurementVisitor;

import org.junit.Assert;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class MockSchemaTreeMeasurementVisitor extends SchemaTreeMeasurementVisitor {
  Map<SchemaNode, Integer> map = new HashMap<>();

  @Override
  protected MeasurementPath generateResult(SchemaNode nextMatchedNode) {
    Assert.assertTrue(map.get(nextMatchedNode) > 0);
    return super.generateResult(nextMatchedNode);
  }

  public MockSchemaTreeMeasurementVisitor(
      SchemaNode root, PartialPath pathPattern, boolean isPrefixMatch) {
    super(root, pathPattern, isPrefixMatch);
  }

  @Override
  protected Iterator<SchemaNode> getChildrenIterator(SchemaNode parent) {
    return new CountIterator(super.getChildrenIterator(parent));
  }

  @Override
  protected SchemaNode getChild(SchemaNode parent, String childName) {
    SchemaNode node = super.getChild(parent, childName);
    if (node != null) {
      if (map.containsKey(node)) {
        map.put(node, map.get(node) + 1);
      } else {
        map.put(node, 1);
      }
    }
    return node;
  }

  @Override
  protected void releaseNode(SchemaNode child) {
    map.computeIfPresent(child, (node, cnt) -> cnt - 1);
  }

  @Override
  protected void releaseNodeIterator(Iterator<SchemaNode> nodeIterator) {
    super.releaseNodeIterator(nodeIterator);
  }

  @Override
  public void close() {
    super.close();
    for (int cnt : map.values()) {
      Assert.assertEquals(0, cnt);
    }
  }

  private class CountIterator implements Iterator<SchemaNode> {
    Iterator<SchemaNode> iterator;

    private CountIterator(Iterator<SchemaNode> iterator) {
      this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public SchemaNode next() {
      SchemaNode node = iterator.next();
      if (map.containsKey(node)) {
        map.put(node, map.get(node) + 1);
      } else {
        map.put(node, 1);
      }
      return node;
    }
  }
}
