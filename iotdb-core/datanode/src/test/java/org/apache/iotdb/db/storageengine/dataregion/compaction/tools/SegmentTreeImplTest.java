package org.apache.iotdb.db.storageengine.dataregion.compaction.tools;

import org.apache.iotdb.db.storageengine.dataregion.compaction.tool.Interval;
import org.apache.iotdb.db.storageengine.dataregion.compaction.tool.SegmentTreeImpl;

import org.junit.Test;

public class SegmentTreeImplTest {
  @Test
  public void test01() {
    SegmentTreeImpl segmentTree = new SegmentTreeImpl();
    segmentTree.insert(new Interval(15, 20));
    segmentTree.insert(new Interval(10, 30));
    segmentTree.insert(new Interval(17, 19));
    segmentTree.insert(new Interval(5, 20));

    System.out.println(segmentTree);
  }
}
