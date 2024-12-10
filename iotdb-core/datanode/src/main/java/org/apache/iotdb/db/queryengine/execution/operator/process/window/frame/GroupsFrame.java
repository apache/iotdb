package org.apache.iotdb.db.queryengine.execution.operator.process.window.frame;

import org.apache.iotdb.db.queryengine.execution.operator.process.window.utils.Range;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.utils.RowComparator;
import org.apache.tsfile.block.column.Column;

import java.util.List;

public class GroupsFrame implements Frame {
  private final FrameInfo frameInfo;
  private final int partitionStart;
  private final int partitionEnd;

  private final List<Column> columns;
  private final RowComparator peerGroupComparator;

  private Range recentRange;
  private int recentStartPeerGroup;
  private int recentEndPeerGroup;
  private boolean frameStartFollowingReachEnd = false;


  public GroupsFrame(
      FrameInfo frameInfo, int partitionStart, int partitionEnd, List<Column> columns, RowComparator peerGroupComparator, int initialEnd) {
    this.frameInfo = frameInfo;
    this.partitionStart = partitionStart;
    this.partitionEnd = partitionEnd;
    this.columns = columns;
    this.peerGroupComparator = peerGroupComparator;

    this.recentRange = new Range(0, initialEnd);
    this.recentStartPeerGroup = 0;
    this.recentEndPeerGroup = 0;
  }

  @Override
  public Range getRange(
      int currentPosition, int currentGroup, int peerGroupStart, int peerGroupEnd) {
    int frameStart = -1;
    FrameInfo.FrameBoundType startType = frameInfo.getStartType();
    if (startType == FrameInfo.FrameBoundType.UNBOUNDED_PRECEDING) {
      frameStart = 0;
    } else if (startType == FrameInfo.FrameBoundType.PRECEDING) {
      frameStart = getStartPrecedingOffset(currentGroup) - partitionStart;
    } else if (startType == FrameInfo.FrameBoundType.CURRENT_ROW) {
      frameStart = peerGroupStart - partitionStart;
    } else if (startType == FrameInfo.FrameBoundType.FOLLOWING) {
      frameStart = getStartFollowingOffset(currentGroup) - partitionStart;
    } // UNBOUND_FOLLOWING is not allowed in frame start

    int frameEnd = -1;
    FrameInfo.FrameBoundType endType = frameInfo.getEndType();
    if (endType == FrameInfo.FrameBoundType.PRECEDING) {
      frameEnd = getEndPrecedingOffset(currentGroup) - partitionStart;
    } else if (endType == FrameInfo.FrameBoundType.CURRENT_ROW) {
      frameEnd = peerGroupEnd - partitionStart - 1;
    } else if (endType == FrameInfo.FrameBoundType.FOLLOWING) {
      frameEnd = getEndFollowingOffset(currentGroup) - partitionStart;
    } else if (endType == FrameInfo.FrameBoundType.UNBOUNDED_FOLLOWING) {
      frameEnd = partitionEnd - partitionStart - 1;
    } // UNBOUND_PRECEDING is not allowed in frame end

    recentRange = new Range(frameStart, frameEnd);
    return recentRange;
  }

  private int getStartPrecedingOffset(int currentGroup) {
    int start = recentRange.getStart()  + partitionStart;
    int offset = (int) frameInfo.getStartOffset();
    if (currentGroup - offset > recentStartPeerGroup) {
      int count = currentGroup - offset - recentStartPeerGroup;
      for (int i = 0; i < count; i++) {
        // Scan over current peer group
        start = scanPeerGroup(start);
        // Enter next peer group(won't reach partition end)
        start++;
      }
      recentStartPeerGroup = currentGroup - offset;
    }

    return start;
  }

  private int getEndPrecedingOffset(int currentGroup) {
    int end = recentRange.getEnd()  + partitionStart;
    int offset = (int) frameInfo.getEndOffset();
    if (currentGroup - offset > recentStartPeerGroup) {
      int count = currentGroup - offset - recentStartPeerGroup;
      for (int i = 0; i < count; i++) {
        // Enter next peer group
        end++;
        // Scan over current peer group(won't reach partition end)
        end = scanPeerGroup(end);
      }
      recentStartPeerGroup = currentGroup - offset;
    }

    return end;
  }

  private int getStartFollowingOffset(int currentGroup) {
    int start = recentRange.getStart() + partitionStart;
    // Shortcut if we have reached last peer group already
    if (frameStartFollowingReachEnd) {
      return start;
    }

    int offset = (int) frameInfo.getStartOffset();
    if (currentGroup + offset > recentEndPeerGroup) {
      int count = currentGroup + offset - recentStartPeerGroup;
      for (int i = 0; i < count; i++) {
        int prev = start;
        // Scan over current peer group
        start = scanPeerGroup(start);
        // Enter next peer group
        if (start == partitionEnd - 1) {
          // Reach partition end
          recentEndPeerGroup = currentGroup + i;
          frameStartFollowingReachEnd = true;
          return prev;
        } else {
          start++;
        }
      }
      recentStartPeerGroup = currentGroup + offset;
    }

    return start;
  }

  private int getEndFollowingOffset(int currentGroup) {
    int end = recentRange.getEnd()  + partitionStart;
    // Shortcut if we have reached partition end already
    if (end == partitionEnd - 1) {
      return end;
    }

    int offset = (int) frameInfo.getEndOffset();
    if (currentGroup + offset > recentStartPeerGroup) {
      int count = currentGroup + offset - recentStartPeerGroup;
      for (int i = 0; i < count; i++) {
        // Enter next peer group
        if (end == partitionEnd - 1) {
          // Reach partition end
          recentEndPeerGroup = currentGroup + i;
          return end;
        }
        end++;
        // Scan over current peer group
        end = scanPeerGroup(end);
      }
      recentStartPeerGroup = currentGroup + offset;
    }

    return end;
  }

  private int scanPeerGroup(int currentPosition) {
    while (currentPosition < partitionEnd - 1 && peerGroupComparator.equal(columns, currentPosition, currentPosition + 1)) {
      currentPosition++;
    }
    return currentPosition;
  }
}
