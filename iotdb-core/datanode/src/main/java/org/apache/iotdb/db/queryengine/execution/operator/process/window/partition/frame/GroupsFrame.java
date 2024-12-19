package org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.frame;

import org.apache.iotdb.db.queryengine.execution.operator.process.window.exception.FrameTypeException;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.utils.ColumnList;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.utils.Range;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.utils.RowComparator;

import com.google.common.collect.ImmutableList;

import java.util.List;

public class GroupsFrame implements Frame {
  private final FrameInfo frameInfo;
  private final int partitionStart;
  private final int partitionEnd;
  private final int partitionSize;

  private final List<ColumnList> columns;
  private final RowComparator peerGroupComparator;

  private Range recentRange;
  private int recentStartPeerGroup;
  private int recentEndPeerGroup;
  private boolean frameStartFollowingReachEnd = false;

  public GroupsFrame(
      FrameInfo frameInfo,
      int partitionStart,
      int partitionEnd,
      List<ColumnList> columns,
      RowComparator peerGroupComparator,
      int initialEnd) {
    this.frameInfo = frameInfo;
    this.partitionStart = partitionStart;
    this.partitionEnd = partitionEnd;
    this.partitionSize = partitionEnd - partitionStart;
    this.columns = ImmutableList.copyOf(columns);
    this.peerGroupComparator = peerGroupComparator;

    this.recentRange = new Range(0, initialEnd);
    this.recentStartPeerGroup = 0;
    this.recentEndPeerGroup = 0;
  }

  @Override
  public Range getRange(
      int currentPosition, int currentGroup, int peerGroupStart, int peerGroupEnd) {
    int frameStart;
    switch (frameInfo.getStartType()) {
      case UNBOUNDED_PRECEDING:
        frameStart = 0;
        break;
      case PRECEDING:
        frameStart = getStartPrecedingOffset(currentGroup);
        break;
      case CURRENT_ROW:
        frameStart = peerGroupStart - partitionStart;
        break;
      case FOLLOWING:
        frameStart = getStartFollowingOffset(currentGroup);
        break;
      default:
        // UNBOUND_FOLLOWING is not allowed in frame start
        throw new FrameTypeException(true);
    }

    int frameEnd;
    switch (frameInfo.getEndType()) {
      case PRECEDING:
        frameEnd = getEndPrecedingOffset(currentGroup);
        break;
      case CURRENT_ROW:
        frameEnd = peerGroupEnd - partitionStart - 1;
        break;
      case FOLLOWING:
        frameEnd = getEndFollowingOffset(currentGroup);
        break;
      case UNBOUNDED_FOLLOWING:
        frameEnd = partitionEnd - partitionStart - 1;
        break;
      default:
        // UNBOUND_PRECEDING is not allowed in frame end
        throw new FrameTypeException(false);
    }

    // Empty frame
    if (frameEnd < frameStart || frameEnd < 0 || frameStart >= partitionSize) {
      return new Range(-1, -1);
    }

    frameStart = Math.max(frameStart, 0);
    frameEnd = Math.min(frameEnd, partitionSize - 1);
    recentRange = new Range(frameStart, frameEnd);
    return recentRange;
  }

  private int getStartPrecedingOffset(int currentGroup) {
    int start = recentRange.getStart() + partitionStart;
    int offset = (int) frameInfo.getStartOffset();

    // We may encounter empty frame
    if (currentGroup - offset < 0) {
      return -1;
    }

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

    return start - partitionStart;
  }

  private int getEndPrecedingOffset(int currentGroup) {
    int end = recentRange.getEnd() + partitionStart;
    int offset = (int) frameInfo.getEndOffset();

    // We may encounter empty frame
    if (currentGroup - offset < 0) {
      return -1;
    }

    if (currentGroup - offset > recentEndPeerGroup) {
      int count = currentGroup - offset - recentEndPeerGroup;
      for (int i = 0; i < count; i++) {
        // Enter next peer group
        end++;
        // Scan over current peer group(won't reach partition end)
        end = scanPeerGroup(end);
      }
      recentEndPeerGroup = currentGroup - offset;
    }

    return end - partitionStart;
  }

  private int getStartFollowingOffset(int currentGroup) {
    // Shortcut if we have reached last peer group already
    if (frameStartFollowingReachEnd) {
      return partitionEnd;
    }

    int start = recentRange.getStart() + partitionStart;

    int offset = (int) frameInfo.getStartOffset();
    if (currentGroup + offset > recentStartPeerGroup) {
      int count = currentGroup + offset - recentStartPeerGroup;
      for (int i = 0; i < count; i++) {
        int prev = start;
        // Scan over current peer group
        start = scanPeerGroup(start);
        // Enter next peer group
        if (start == partitionEnd - 1) {
          // Reach partition end
          // We may encounter empty frame here
          recentStartPeerGroup = currentGroup + i;
          frameStartFollowingReachEnd = true;
          return partitionEnd;
        } else {
          start++;
        }
      }
      recentStartPeerGroup = currentGroup + offset;
    }

    return start - partitionStart;
  }

  private int getEndFollowingOffset(int currentGroup) {
    int end = recentRange.getEnd() + partitionStart;
    // Shortcut if we have reached partition end already
    if (end == partitionEnd - 1) {
      return end;
    }

    int offset = (int) frameInfo.getEndOffset();
    if (currentGroup + offset > recentEndPeerGroup) {
      int count = currentGroup + offset - recentEndPeerGroup;
      for (int i = 0; i < count; i++) {
        // Enter next peer group
        if (end == partitionEnd - 1) {
          if (i != count - 1) {
            // Too early, we may encounter empty frame
            return partitionEnd;
          }

          // Reach partition end
          recentEndPeerGroup = currentGroup + i;
          return end;
        }
        end++;
        // Scan over current peer group
        end = scanPeerGroup(end);
      }
      recentEndPeerGroup = currentGroup + offset;
    }

    return end - partitionStart;
  }

  private int scanPeerGroup(int currentPosition) {
    while (currentPosition < partitionEnd - 1
        && peerGroupComparator.equalColumnLists(columns, currentPosition, currentPosition + 1)) {
      currentPosition++;
    }
    return currentPosition;
  }
}
