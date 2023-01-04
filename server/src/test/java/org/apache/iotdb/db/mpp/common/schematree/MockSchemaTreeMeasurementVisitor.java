package org.apache.iotdb.db.mpp.common.schematree;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.mpp.common.schematree.node.SchemaNode;
import org.apache.iotdb.db.mpp.common.schematree.visitor.SchemaTreeMeasurementVisitor;

import org.junit.Assert;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class MockSchemaTreeMeasurementVisitor extends SchemaTreeMeasurementVisitor {
  Map<SchemaNode, Integer> map = new HashMap<>();

  public MockSchemaTreeMeasurementVisitor(
      SchemaNode root, PartialPath pathPattern, int slimit, int soffset, boolean isPrefixMatch) {
    super(root, pathPattern, slimit, soffset, isPrefixMatch);
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

  public void check() {
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
