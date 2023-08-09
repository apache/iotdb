package org.apache.iotdb.db.storageengine.dataregion.compaction.tool;

public class SegmentTreeImpl implements ITimeRange {

  private IntervalNode root;

  @Override
  public void addInterval(Interval interval) {
    root = insert(root, interval);
  }

  @Override
  public boolean isOverlapped(Interval interval) {
    return overlaps(root, interval);
  }

  private boolean overlaps(IntervalNode node, Interval interval) {
    if (node == null) {
      return false;
    }

    if (node.interval.getStart() <= interval.getEnd()
        && interval.getStart() <= node.interval.getEnd()) {
      return true;
    }

    if (node.left != null && node.left.maxEnd >= interval.getStart()) {
      return overlaps(node.left, interval);
    }

    return overlaps(node.right, interval);
  }

  public void insert(Interval interval) {
    root = insert(root, interval);
  }

  private IntervalNode insert(IntervalNode node, Interval interval) {
    if (node == null) {
      return new IntervalNode(interval);
    }

    long start = interval.getStart();
    if (start < node.interval.getStart()) {
      node.left = insert(node.left, interval);
    } else {
      node.right = insert(node.right, interval);
    }

    node.maxEnd = Math.max(node.maxEnd, interval.getEnd());
    return node;
  }

  private class IntervalNode {
    Interval interval;
    long maxEnd;
    IntervalNode left;
    IntervalNode right;

    public IntervalNode(Interval interval) {
      this.interval = interval;
      this.maxEnd = interval.getEnd();
    }
  }
}
