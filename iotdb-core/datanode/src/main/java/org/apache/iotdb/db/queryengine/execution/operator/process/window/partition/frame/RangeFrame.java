package org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.frame;

import org.apache.iotdb.db.queryengine.execution.operator.process.window.exception.FrameTypeException;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.utils.ColumnList;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.utils.Range;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.utils.RowComparator;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.util.List;

import static org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.frame.FrameInfo.FrameBoundType.CURRENT_ROW;
import static org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.frame.FrameInfo.FrameBoundType.FOLLOWING;
import static org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.frame.FrameInfo.FrameBoundType.PRECEDING;
import static org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.frame.FrameInfo.FrameBoundType.UNBOUNDED_FOLLOWING;
import static org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.frame.FrameInfo.FrameBoundType.UNBOUNDED_PRECEDING;

public class RangeFrame implements Frame {
  private final FrameInfo frameInfo;
  private final ColumnList column;
  private final TSDataType dataType;

  private final int partitionStart;
  private final int partitionEnd;
  private final int partitionSize;
  private final RowComparator peerGroupComparator;
  private Range recentRange;

  public RangeFrame(
      FrameInfo frameInfo,
      int partitionStart,
      int partitionEnd,
      List<ColumnList> columns,
      RowComparator comparator) {
    this.frameInfo = frameInfo;
    // Only one sort key is allowed in range frame
    assert columns.size() == 1;
    this.column = columns.get(0);
    this.dataType = column.getDataType();
    this.partitionStart = partitionStart;
    this.partitionEnd = partitionEnd;
    this.partitionSize = partitionEnd - partitionStart;
    this.peerGroupComparator = comparator;
    this.recentRange = new Range(0, 0);
  }

  @Override
  public Range getRange(
      int currentPosition, int currentGroup, int peerGroupStart, int peerGroupEnd) {
    // Full partition
    if (frameInfo.getStartType() == UNBOUNDED_PRECEDING
        && frameInfo.getEndType() == UNBOUNDED_FOLLOWING) {
      return new Range(0, partitionEnd - partitionStart - 1);
    }

    // Peer group
    if (frameInfo.getStartType() == CURRENT_ROW && frameInfo.getEndType() == CURRENT_ROW
        || frameInfo.getStartType() == CURRENT_ROW && frameInfo.getEndType() == UNBOUNDED_FOLLOWING
        || frameInfo.getStartType() == UNBOUNDED_PRECEDING
            && frameInfo.getEndType() == CURRENT_ROW) {
      if (currentPosition == 0
          || !peerGroupComparator.equal(column, currentPosition - 1, currentPosition)) {
        // New peer group
        int frameStart =
            frameInfo.getStartType() == CURRENT_ROW ? (peerGroupStart - partitionStart) : 0;
        int frameEnd =
            frameInfo.getEndType() == CURRENT_ROW
                ? (peerGroupEnd - partitionStart - 1)
                : (partitionEnd - partitionStart - 1);

        recentRange = new Range(frameStart, frameEnd);
      }
      // Old peer group is considered as well
      return recentRange;
    }

    // Current row is NULL
    // According to Spec, behavior of "X PRECEDING", "X FOLLOWING" frame boundaries is similar to
    // "CURRENT ROW" for null values.
    if (column.isNull(currentPosition)) {
      recentRange =
          new Range(
              frameInfo.getStartType() == UNBOUNDED_PRECEDING ? 0 : peerGroupStart - partitionStart,
              frameInfo.getEndType() == UNBOUNDED_FOLLOWING
                  ? partitionEnd - partitionStart - 1
                  : peerGroupEnd - partitionStart - 1);
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
        frameStart = getPrecedingOffset(currentPosition, peerGroupStart, peerGroupEnd, true);
        break;
      case CURRENT_ROW:
        frameStart = peerGroupStart - partitionStart;
        break;
      case FOLLOWING:
        frameStart = getFollowingOffset(currentPosition, peerGroupStart, peerGroupEnd, true);
        break;
      default:
        // UNBOUND_FOLLOWING is not allowed in frame start
        throw new FrameTypeException(true);
    }

    int frameEnd;
    switch (frameInfo.getEndType()) {
      case PRECEDING:
        frameEnd = getPrecedingOffset(currentPosition, peerGroupStart, peerGroupEnd, false);
        break;
      case CURRENT_ROW:
        frameEnd = peerGroupEnd - partitionStart - 1;
        break;
      case FOLLOWING:
        frameEnd = getFollowingOffset(currentPosition, peerGroupStart, peerGroupEnd, false);
        break;
      case UNBOUNDED_FOLLOWING:
        frameEnd = partitionEnd - partitionStart - 1;
        break;
      default:
        // UNBOUND_PRECEDING is not allowed in frame start
        throw new FrameTypeException(false);
    }

    if (frameEnd < frameStart || frameEnd < 0 || frameStart >= partitionSize) {
      recentRange = new Range(Math.min(partitionEnd - partitionStart - 1, frameStart),
          Math.max(0, frameEnd));
      return new Range(-1, -1);
    }

    frameStart = Math.max(frameStart, 0);
    frameEnd = Math.min(frameEnd, partitionSize - 1);
    recentRange = new Range(frameStart, frameEnd);
    return recentRange;
  }

  private int getPrecedingOffset(int index, int peerGroupStart, int peerGroupEnd, boolean isStart) {
    int offset;
    if (isStart) {
      if (!dataType.isNumeric() && dataType != TSDataType.DATE) {
        return peerGroupStart;
      }

      double offsetStart = frameInfo.getStartOffset();
      int recentStart = recentRange.getStart() + partitionStart;

      // Recent start from NULL
      // Which means current row is the first non-null row
      if (frameInfo.getSortOrder().isNullsFirst() && column.isNull(recentStart)) {
        // Then the frame starts with current row
        return index - partitionStart;
      }

      if (frameInfo.getSortOrder().isAscending()) {
        offset = getAscFrameStartPreceding(index, recentStart, offsetStart);
      } else {
        offset = getDescFrameStartPreceding(index, recentStart, offsetStart);
      }
    } else {
      if (!dataType.isNumeric() && dataType != TSDataType.DATE) {
        return peerGroupEnd;
      }

      double offsetEnd = frameInfo.getEndOffset();
      int recentEnd = recentRange.getEnd() + partitionStart;

      // Leave section of leading nulls
      if (frameInfo.getSortOrder().isNullsFirst()) {
        while (recentEnd < partitionEnd && column.isNull(recentEnd)) {
          recentEnd++;
        }
      }

      if (frameInfo.getSortOrder().isAscending()) {
        offset = getAscFrameEndPreceding(index, recentEnd, offsetEnd);
      } else {
        offset = getDescFrameEndPreceding(index, recentEnd, offsetEnd);
      }
    }
    offset -= partitionStart;

    return offset;
  }

  private int getFollowingOffset(int index, int peerGroupStart, int peerGroupEnd, boolean isStart) {
    int offset;
    if (isStart) {
      if (!dataType.isNumeric() && dataType != TSDataType.DATE) {
        return peerGroupStart;
      }

      double offsetStart = frameInfo.getStartOffset();
      int recentStart = recentRange.getStart() + partitionStart;

      // Leave section of leading nulls
      if (recentStart == partitionStart && frameInfo.getSortOrder().isNullsFirst() && column.isNull(partitionStart)) {
        // Then the frame starts with current row
        recentStart = index - partitionStart;
      }

      // Leave section of tailing nulls
      if (!frameInfo.getSortOrder().isNullsFirst()) {
        while (recentStart >= 0 && column.isNull(recentStart)) {
          recentStart--;
        }
        if (recentStart < 0) {
          return recentStart - partitionStart;
        }
      }

      if (frameInfo.getSortOrder().isAscending()) {
        offset = getAscFrameStartFollowing(index, recentStart, offsetStart);
      } else {
        offset = getDescFrameStartFollowing(index, recentStart, offsetStart);
      }
    } else {
      if (!dataType.isNumeric() && dataType != TSDataType.DATE) {
        return peerGroupEnd;
      }

      double offsetEnd = frameInfo.getEndOffset();
      int recentEnd = recentRange.getEnd() + partitionStart;

      // Leave section of leading nulls
      if (frameInfo.getSortOrder().isNullsFirst() && column.isNull(recentEnd)) {
        // Then the frame starts with current row
        recentEnd = index - partitionStart;
      }

      if (frameInfo.getSortOrder().isAscending()) {
        offset = getAscFrameEndFollowing(index, recentEnd, offsetEnd);
      } else {
        offset = getDescFrameEndFollowing(index, recentEnd, offsetEnd);
      }
    }
    offset -= partitionStart;

    return offset;
  }

  // Find first row which satisfy:
  // follow >= current + offset
  // And stop right there
  private int getAscFrameStartFollowing(int currentIndex, int recentIndex, double offset) {
    while (recentIndex < partitionEnd && !column.isNull(recentIndex)) {
      double current = getColumnValueAsDouble(column, currentIndex);
      double follow = getColumnValueAsDouble(column, recentIndex);
      if (follow >= current + offset) {
        return recentIndex;
      }
      recentIndex++;
    }
    return recentIndex;
  }

  // Find first row which satisfy:
  // follow > current + offset
  // And return its previous index
  private int getAscFrameEndFollowing(int currentIndex, int recentIndex, double offset) {
    while (recentIndex < partitionEnd && !column.isNull(recentIndex)) {
      double current = getColumnValueAsDouble(column, currentIndex);
      double follow = getColumnValueAsDouble(column, recentIndex);
      if (follow > current + offset) {
        return recentIndex - 1;
      }
      recentIndex++;
    }
    return recentIndex - 1;
  }

  // Find first row which satisfy:
  // precede >= current - offset
  // And stop right there
  private int getAscFrameStartPreceding(int currentIndex, int recentIndex, double offset) {
    while (recentIndex < currentIndex) {
      double current = getColumnValueAsDouble(column, currentIndex);
      double precede = getColumnValueAsDouble(column, recentIndex);
      if (precede >= current - offset) {
        return recentIndex;
      }
      recentIndex++;
    }
    return recentIndex;
  }

  // Find first row which satisfy:
  // precede > current - offset
  // And return its previous index
  private int getAscFrameEndPreceding(int currentIndex, int recentIndex, double offset) {
    while (recentIndex < partitionEnd) {
      double current = getColumnValueAsDouble(column, currentIndex);
      double precede = getColumnValueAsDouble(column, recentIndex);
      if (precede > current - offset) {
        return recentIndex - 1;
      }
      recentIndex++;
    }
    return recentIndex - 1;
  }

  // Find first row which satisfy:
  // follow <= current - offset
  // And stop right there
  private int getDescFrameStartFollowing(int currentIndex, int recentIndex, double offset) {
    while (recentIndex < partitionEnd && !column.isNull(recentIndex)) {
      double current = getColumnValueAsDouble(column, currentIndex);
      double follow = getColumnValueAsDouble(column, recentIndex);
      if (follow <= current - offset) {
        return recentIndex;
      }
      recentIndex++;
    }
    return recentIndex;
  }

  // Find first row which satisfy:
  // follow < current - offset
  // And return its previous index
  private int getDescFrameEndFollowing(int currentIndex, int recentIndex, double offset) {
    while (recentIndex < partitionEnd && !column.isNull(recentIndex)) {
      double current = getColumnValueAsDouble(column, currentIndex);
      double follow = getColumnValueAsDouble(column, recentIndex);
      if (follow < current - offset) {
        return recentIndex - 1;
      }
      recentIndex++;
    }
    return recentIndex - 1;
  }

  // Find first row which satisfy:
  // precede <= current + offset
  // And stop right there
  private int getDescFrameStartPreceding(int currentIndex, int recentIndex, double offset) {
    while (recentIndex < currentIndex) {
      double current = getColumnValueAsDouble(column, currentIndex);
      double precede = getColumnValueAsDouble(column, recentIndex);
      if (precede <= current + offset) {
        return recentIndex;
      }
      recentIndex++;
    }
    return recentIndex;
  }

  // Find first row which satisfy:
  // precede < current + offset
  // And return its previous index
  private int getDescFrameEndPreceding(int currentIndex, int recentIndex, double offset) {
    while (recentIndex < partitionEnd) {
      double current = getColumnValueAsDouble(column, currentIndex);
      double precede = getColumnValueAsDouble(column, recentIndex);
      if (precede < current + offset) {
        return recentIndex - 1;
      }
      recentIndex++;
    }
    return recentIndex - 1;
  }

  private double getColumnValueAsDouble(ColumnList column, int index) {
    switch (column.getDataType()) {
      case INT32:
      case DATE:
        return column.getInt(index);
      case INT64:
        return column.getLong(index);
      case FLOAT:
        return column.getFloat(index);
      case DOUBLE:
        return column.getDouble(index);
      default:
        // Unreachable
        throw new UnSupportedDataTypeException("Unsupported data type: " + column.getDataType());
    }
  }
}
