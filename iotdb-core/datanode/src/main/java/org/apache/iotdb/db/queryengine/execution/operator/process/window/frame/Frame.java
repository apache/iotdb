package org.apache.iotdb.db.queryengine.execution.operator.process.window.frame;

public interface Frame {
  Range getRange(int currentPosition, int currentGroup, int peerGroupStart, int peerGroupEnd);

  class Range {
    private final int start;
    private final int end;

    public Range(int start, int end) {
      this.start = start;
      this.end = end;
    }

    public int getStart() {
      return start;
    }

    public int getEnd() {
      return end;
    }
  }
}
