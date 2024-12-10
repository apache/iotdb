package org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.frame;

import org.apache.iotdb.db.queryengine.execution.operator.process.window.exception.FrameTypeException;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.utils.Range;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.utils.RowComparator;
import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;

import java.util.List;

import static org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.frame.FrameInfo.FrameBoundType.CURRENT_ROW;
import static org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.frame.FrameInfo.FrameBoundType.FOLLOWING;
import static org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.frame.FrameInfo.FrameBoundType.PRECEDING;
import static org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.frame.FrameInfo.FrameBoundType.UNBOUNDED_FOLLOWING;
import static org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.frame.FrameInfo.FrameBoundType.UNBOUNDED_PRECEDING;

public class RangeFrame implements Frame {
  private final FrameInfo frameInfo;
  private final Column column;
  private final TSDataType dataType;

  private final int partitionStart;
  private final int partitionEnd;
  private final RowComparator peerGroupComparator;
  private Range recentRange;

  public RangeFrame(
      FrameInfo frameInfo,
      int partitionStart,
      int partitionEnd,
      List<Column> columns,
      RowComparator comparator,
      int initialEnd) {
    this.frameInfo = frameInfo;
    // Only one sort key is allowed in range frame
    assert columns.size() == 1;
    this.column = columns.get(0);
    this.dataType = column.getDataType();
    this.partitionStart = partitionStart;
    this.partitionEnd = partitionEnd;
    this.peerGroupComparator = comparator;
    this.recentRange = new Range(0, initialEnd);
  }

  @Override
  public Range getRange(
      int currentPosition, int currentGroup, int peerGroupStart, int peerGroupEnd) {
    // Full partition
    if (frameInfo.getStartType() == UNBOUNDED_PRECEDING && frameInfo.getEndType() == UNBOUNDED_FOLLOWING) {
      return new Range(0, partitionEnd - partitionStart + 1);
    }

    // Peer group
    if (frameInfo.getStartType() == CURRENT_ROW && frameInfo.getEndType() == CURRENT_ROW ||
        frameInfo.getStartType() == CURRENT_ROW && frameInfo.getEndType() == UNBOUNDED_FOLLOWING ||
        frameInfo.getStartType() == UNBOUNDED_PRECEDING && frameInfo.getEndType() == CURRENT_ROW) {
      if (currentPosition != 0 && !peerGroupComparator.equal(column, currentPosition, peerGroupStart)) {
        // New peer group
        int frameStart = frameInfo.getStartType() == CURRENT_ROW? (peerGroupStart - partitionStart) : 0;
        int frameEnd = frameInfo.getEndType() == CURRENT_ROW? (peerGroupEnd - partitionStart - 1) : (partitionEnd - partitionStart - 1);

        recentRange = new Range(frameStart, frameEnd);
      }
      // Old peer group is considered as well
      return recentRange;
    }

    // Current row is NULL
    // According to Spec, behavior of "X PRECEDING", "X FOLLOWING" frame boundaries is similar to "CURRENT ROW" for null values.
    if (column.isNull(currentPosition)) {
      recentRange = new Range(
          frameInfo.getStartType() == UNBOUNDED_PRECEDING ? 0 : peerGroupStart - partitionStart,
          frameInfo.getEndType() == UNBOUNDED_FOLLOWING ? partitionEnd - partitionStart - 1 : peerGroupEnd - partitionStart - 1);
      return recentRange;
    }

    // Current row is not NULL
    // Frame definition has at least one of: X PRECEDING, Y FOLLOWING
    int frameStart;
    switch (frameInfo.getStartType()) {
      case UNBOUNDED_PRECEDING:
        frameStart = 0;
        break;
      case PRECEDING:
        frameStart = getPrecedingOffset(currentPosition, peerGroupStart, true);
        break;
      case CURRENT_ROW:
        frameStart = peerGroupStart - partitionStart;
        break;
      case FOLLOWING:
        frameStart = getFollowingOffset(currentPosition, peerGroupEnd, true);
        break;
      default:
        // UNBOUND_FOLLOWING is not allowed in frame start
        throw new FrameTypeException(true);
    }

    int frameEnd;
    switch (frameInfo.getEndType()) {
      case PRECEDING:
        frameEnd = getPrecedingOffset(currentPosition, peerGroupStart, false);
        break;
      case CURRENT_ROW:
        frameEnd = peerGroupEnd - partitionStart - 1;
        break;
      case FOLLOWING:
        frameEnd = getFollowingOffset(currentPosition, peerGroupEnd, false);
        break;
      case UNBOUNDED_FOLLOWING:
        frameEnd = partitionEnd - partitionStart - 1;
        break;
      default:
        // UNBOUND_PRECEDING is not allowed in frame start
        throw new FrameTypeException(false);
    }

    // Special case
    if (frameInfo.getStartType() == PRECEDING && frameInfo.getEndType() == PRECEDING && frameEnd == currentPosition
        || frameInfo.getStartType() == FOLLOWING && frameInfo.getEndType() == FOLLOWING &&  frameEnd == currentPosition) {
      frameEnd--;
      // Empty frame
      if (frameStart > frameEnd) {
        return new Range(-1, -1);
      }
    }

    recentRange = new Range(frameStart, frameEnd);
    return recentRange;
  }

  private int getPrecedingOffset(int currentPosition, int peerGroupStart, boolean isStart) {
    // For non-numeric types
    // Preceding offset is peer group start
    if (!dataType.isNumeric()) {
      return peerGroupStart;
    }

    // Optimize with cached range
    int recentStart = recentRange.getStart() + partitionStart;

    // Recent start from NULL
    // Which means current row is the first non-null row
    if (frameInfo.getSortOrder().isNullsFirst() && column.isNull(recentStart)) {
      // Then the frame starts with current row
      return currentPosition - partitionStart;
    }

    if (frameInfo.getSortOrder().isAscending()) {
      return getAscPrecedingOffset(currentPosition, recentStart, isStart) - partitionStart;
    } else {
      return getDescPrecedingOffset(currentPosition, recentStart, isStart) - partitionStart;
    }
  }

  private int getFollowingOffset(int currentPosition, int peerGroupEnd, boolean isStart) {
    // For non-numeric types
    // Following offset is peer group end
    if (!dataType.isNumeric()) {
      return peerGroupEnd;
    }

    // Optimize with cached range
    int recentEnd = recentRange.getEnd() + partitionStart;

    if (frameInfo.getSortOrder().isAscending()) {
      return getAscFollowingOffset(currentPosition, recentEnd, isStart) - partitionStart;
    } else {
      return getDescFollowingOffset(currentPosition, recentEnd, isStart) - partitionStart;
    }
  }

  private int getAscPrecedingOffset(int currentPosition, int recentStart, boolean isStart) {
    // Find first row which satisfy:
    // precede >= current - offset
    double offset = isStart? frameInfo.getStartOffset() : frameInfo.getEndOffset();
    while (recentStart < currentPosition) {
      switch (column.getDataType()) {
        case INT32: {
          int current = column.getInt(currentPosition);
          int precede = column.getInt(recentStart);
          if (precede >= current - offset) {
            return recentStart;
          }
          break;
        }
        case INT64: {
          long current = column.getLong(currentPosition);
          long precede = column.getLong(recentStart);
          if (precede >= current - offset) {
            return recentStart;
          }
          break;
        }
        case FLOAT: {
          float current = column.getFloat(currentPosition);
          float precede = column.getFloat(recentStart);
          if (precede >= current - offset) {
            return recentStart;
          }
          break;
        }
        case DOUBLE: {
          double current = column.getDouble(currentPosition);
          double precede = column.getDouble(recentStart);
          if (precede >= current - offset) {
            return recentStart;
          }
          break;
        }
        default:
          // Unreachable
          break;
      }
      recentStart++;
    }

    return recentStart;
  }

  private int getDescPrecedingOffset(int currentPosition, int recentStart, boolean isStart) {
    // Find first row which satisfy:
    // precede <= current + offset
    double offset = isStart? frameInfo.getStartOffset() : frameInfo.getEndOffset();
    while (recentStart < currentPosition) {
      switch (column.getDataType()) {
        case INT32: {
          int current = column.getInt(currentPosition);
          int precede = column.getInt(recentStart);
          if (precede <= current + offset) {
            return recentStart;
          }
          break;
        }
        case INT64: {
          long current = column.getLong(currentPosition);
          long precede = column.getLong(recentStart);
          if (precede <= current + offset) {
            return recentStart;
          }
          break;
        }
        case FLOAT: {
          float current = column.getFloat(currentPosition);
          float precede = column.getFloat(recentStart);
          if (precede <= current + offset) {
            return recentStart;
          }
          break;
        }
        case DOUBLE: {
          double current = column.getDouble(currentPosition);
          double precede = column.getDouble(recentStart);
          if (precede <= current + offset) {
            return recentStart;
          }
          break;
        }
        default:
          // Unreachable
          break;
      }
      recentStart++;
    }

    return recentStart;
  }

  private int getAscFollowingOffset(int currentPosition, int recentEnd, boolean isStart) {
    // Find first row which satisfy:
    // follow <= current + offset
    double offset = isStart? frameInfo.getStartOffset() : frameInfo.getEndOffset();
    while (recentEnd < partitionEnd) {
      switch (column.getDataType()) {
        case INT32: {
          int current = column.getInt(currentPosition);
          int follow = column.getInt(recentEnd);
          if (follow <= current + offset) {
            return recentEnd;
          }
          break;
        }
        case INT64: {
          long current = column.getLong(currentPosition);
          long follow = column.getLong(recentEnd);
          if (follow <= current + offset) {
            return recentEnd;
          }
          break;
        }
        case FLOAT: {
          float current = column.getFloat(currentPosition);
          float follow = column.getFloat(recentEnd);
          if (follow <= current + offset) {
            return recentEnd;
          }
          break;
        }
        case DOUBLE: {
          double current = column.getDouble(currentPosition);
          double follow = column.getDouble(recentEnd);
          if (follow <= current + offset) {
            return recentEnd;
          }
          break;
        }
        default:
          // Unreachable
          break;
      }
      // Reach tail NULLs
      if (!frameInfo.getSortOrder().isNullsFirst() && column.isNull(recentEnd + 1)) {
        return recentEnd;
      }
      recentEnd++;
    }

    // Inclusive
    return recentEnd - 1;
  }

  private int getDescFollowingOffset(int currentPosition, int recentEnd, boolean isStart) {
    // Find first row which satisfy:
    // follow >= current - offset
    double offset = isStart? frameInfo.getStartOffset() : frameInfo.getEndOffset();
    while (recentEnd < partitionEnd) {
      switch (column.getDataType()) {
        case INT32: {
          int current = column.getInt(currentPosition);
          int follow = column.getInt(recentEnd);
          if (follow >= current - offset) {
            return recentEnd;
          }
          break;
        }
        case INT64: {
          long current = column.getLong(currentPosition);
          long follow = column.getLong(recentEnd);
          if (follow >= current - offset) {
            return recentEnd;
          }
          break;
        }
        case FLOAT: {
          float current = column.getFloat(currentPosition);
          float follow = column.getFloat(recentEnd);
          if (follow >= current - offset) {
            return recentEnd;
          }
          break;
        }
        case DOUBLE: {
          double current = column.getDouble(currentPosition);
          double follow = column.getDouble(recentEnd);
          if (follow >= current - offset) {
            return recentEnd;
          }
          break;
        }
        default:
          // Unreachable
          break;
      }
      // Reach tail NULLs
      if (!frameInfo.getSortOrder().isNullsFirst() && column.isNull(recentEnd + 1)) {
        return recentEnd;
      }
      recentEnd++;
    }

    // Inclusive
    return recentEnd - 1;
  }
}
