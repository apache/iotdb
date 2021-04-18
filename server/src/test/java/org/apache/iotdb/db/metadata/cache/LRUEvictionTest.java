package org.apache.iotdb.db.metadata.cache;

import org.apache.iotdb.db.metadata.mnode.MNode;

import org.junit.Test;

import java.util.Collection;

public class LRUEvictionTest {

  @Test
  public void testLRUEviction() {
    MNode root = getSimpleTree();
    LRUEviction lruEviction = new LRUEviction();
    lruEviction.applyChange(root);
    lruEviction.applyChange(root.getChild("s1"));
    lruEviction.applyChange(root.getChild("s2"));
    lruEviction.applyChange(root.getChild("s1").getChild("t2"));
    StringBuilder stringBuilder = new StringBuilder();
    EvictionEntry entry = root.getEvictionEntry();
    while (entry != null) {
      stringBuilder.append(entry.getValue().getFullPath()).append("\r\n");
      entry = entry.getPre();
    }
    System.out.println(stringBuilder.toString());

    lruEviction.remove(root.getChild("s1"));
    stringBuilder = new StringBuilder();
    entry = root.getEvictionEntry();
    while (entry != null) {
      stringBuilder.append(entry.getValue().getFullPath()).append("\r\n");
      entry = entry.getPre();
    }
    System.out.println(stringBuilder.toString());

    Collection<MNode> collection=lruEviction.evict();
    for(MNode mNode:collection){
      System.out.println(mNode.getFullPath());
    }
  }

  private MNode getSimpleTree() {
    MNode root = new MNode(null, "root");
    root.addChild("s1", new MNode(root, "s1"));
    root.addChild("s2", new MNode(root, "s2"));
    root.getChild("s1").addChild("t1", new MNode(root.getChild("s1"), "t1"));
    root.getChild("s1").addChild("t2", new MNode(root.getChild("s1"), "t2"));
    root.getChild("s1")
        .getChild("t2")
        .addChild("z1", new MNode(root.getChild("s1").getChild("t2"), "z1"));
    root.getChild("s2").addChild("t1", new MNode(root.getChild("s2"), "t1"));
    root.getChild("s2").addChild("t2", new MNode(root.getChild("s2"), "t2"));
    return root;
  }
}
