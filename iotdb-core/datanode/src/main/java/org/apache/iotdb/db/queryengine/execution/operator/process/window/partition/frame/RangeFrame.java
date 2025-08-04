/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.frame;

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.Partition;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.utils.ColumnList;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.utils.Range;
import org.apache.iotdb.db.queryengine.execution.operator.process.window.utils.RowComparator;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.UnSupportedDataTypeException;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.frame.FrameInfo.FrameBoundType.CURRENT_ROW;
import static org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.frame.FrameInfo.FrameBoundType.UNBOUNDED_FOLLOWING;
import static org.apache.iotdb.db.queryengine.execution.operator.process.window.partition.frame.FrameInfo.FrameBoundType.UNBOUNDED_PRECEDING;

public class RangeFrame implements Frame {
  private final Partition partition;
  private final FrameInfo frameInfo;
  private boolean noOrderBy = false;

  private List<ColumnList> allSortedColumns;
  private ColumnList column;
  private TSDataType dataType;

  private int partitionSize;
  private RowComparator peerGroupComparator;
  private Range recentRange;

  public RangeFrame(
      Partition partition,
      FrameInfo frameInfo,
      List<ColumnList> sortedColumns,
      RowComparator comparator) {
    this.partition = partition;
    this.frameInfo = frameInfo;
    this.partitionSize = partition.getPositionCount();

    if (frameInfo.getSortChannel() == -1) {
      // No ORDER BY but uses RANGE frame
      // Return whole partition
      this.noOrderBy = true;
      return;
    }

    // Only one sort key is allowed in range frame
    this.allSortedColumns = sortedColumns;
    this.column = sortedColumns.get(0);
    this.dataType = column.getDataType();
    this.peerGroupComparator = comparator;
    this.recentRange = new Range(0, 0);
  }

  @Override
  public Range getRange(
      int currentPosition, int currentGroup, int peerGroupStart, int peerGroupEnd) {
    // Full partition
    if ((frameInfo.getStartType() == UNBOUNDED_PRECEDING
            && frameInfo.getEndType() == UNBOUNDED_FOLLOWING)
        || noOrderBy) {
      return new Range(0, partitionSize - 1);
    }

    // Peer group
    if (frameInfo.getStartType() == CURRENT_ROW && frameInfo.getEndType() == CURRENT_ROW
        || frameInfo.getStartType() == CURRENT_ROW && frameInfo.getEndType() == UNBOUNDED_FOLLOWING
        || frameInfo.getStartType() == UNBOUNDED_PRECEDING
            && frameInfo.getEndType() == CURRENT_ROW) {
      if (currentPosition == 0
          || !peerGroupComparator.equalColumnLists(
              allSortedColumns, currentPosition - 1, currentPosition)) {
        // New peer group
        int frameStart = frameInfo.getStartType() == CURRENT_ROW ? peerGroupStart : 0;
        int frameEnd = frameInfo.getEndType() == CURRENT_ROW ? peerGroupEnd - 1 : partitionSize - 1;

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
              frameInfo.getStartType() == UNBOUNDED_PRECEDING ? 0 : peerGroupStart,
              frameInfo.getEndType() == UNBOUNDED_FOLLOWING ? partitionSize - 1 : peerGroupEnd - 1);
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
        frameStart = peerGroupStart;
        break;
      case FOLLOWING:
        frameStart = getFollowingOffset(currentPosition, peerGroupStart, peerGroupEnd, true);
        break;
      default:
        // UNBOUND_FOLLOWING is not allowed in frame start
        throw new SemanticException("UNBOUND PRECEDING is not allowed in frame start!");
    }

    int frameEnd;
    switch (frameInfo.getEndType()) {
      case PRECEDING:
        frameEnd = getPrecedingOffset(currentPosition, peerGroupStart, peerGroupEnd, false);
        break;
      case CURRENT_ROW:
        frameEnd = peerGroupEnd - 1;
        break;
      case FOLLOWING:
        frameEnd = getFollowingOffset(currentPosition, peerGroupStart, peerGroupEnd, false);
        break;
      case UNBOUNDED_FOLLOWING:
        frameEnd = partitionSize - 1;
        break;
      default:
        // UNBOUND_PRECEDING is not allowed in frame start
        throw new SemanticException("UNBOUND PRECEDING is not allowed in frame end!");
    }

    if (frameEnd < frameStart || frameEnd < 0 || frameStart >= partitionSize) {
      recentRange = new Range(Math.min(partitionSize - 1, frameStart), Math.max(0, frameEnd));
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
      if (!dataType.isNumeric()
          && dataType != TSDataType.DATE
          && dataType != TSDataType.TIMESTAMP) {
        return peerGroupStart;
      }

      int recentStart = recentRange.getStart();

      // Recent start from NULL
      // Which means current row is the first non-null row
      if (frameInfo.getSortOrder().isNullsFirst() && column.isNull(recentStart)) {
        // Then the frame starts with current row
        return index;
      }

      if (frameInfo.getSortOrder().isAscending()) {
        offset = getAscFrameStartPreceding(index, recentStart);
      } else {
        offset = getDescFrameStartPreceding(index, recentStart);
      }
    } else {
      if (!dataType.isNumeric()
          && dataType != TSDataType.DATE
          && dataType != TSDataType.TIMESTAMP) {
        return peerGroupEnd;
      }

      int recentEnd = recentRange.getEnd();

      // Leave section of leading nulls
      if (frameInfo.getSortOrder().isNullsFirst()) {
        while (recentEnd < partitionSize && column.isNull(recentEnd)) {
          recentEnd++;
        }
      }

      if (frameInfo.getSortOrder().isAscending()) {
        offset = getAscFrameEndPreceding(index, recentEnd);
      } else {
        offset = getDescFrameEndPreceding(index, recentEnd);
      }
    }

    return offset;
  }

  private int getFollowingOffset(int index, int peerGroupStart, int peerGroupEnd, boolean isStart) {
    int offset;
    if (isStart) {
      if (!dataType.isNumeric()
          && dataType != TSDataType.DATE
          && dataType != TSDataType.TIMESTAMP) {
        return peerGroupStart;
      }

      int recentStart = recentRange.getStart();

      // Leave section of leading nulls
      if (recentStart == 0 && frameInfo.getSortOrder().isNullsFirst() && column.isNull(0)) {
        // Then the frame starts with current row
        recentStart = index;
      }

      // Leave section of tailing nulls
      if (!frameInfo.getSortOrder().isNullsFirst()) {
        while (recentStart >= 0 && column.isNull(recentStart)) {
          recentStart--;
        }
        if (recentStart < 0) {
          return recentStart;
        }
      }

      if (frameInfo.getSortOrder().isAscending()) {
        offset = getAscFrameStartFollowing(index, recentStart);
      } else {
        offset = getDescFrameStartFollowing(index, recentStart);
      }
    } else {
      if (!dataType.isNumeric()
          && dataType != TSDataType.DATE
          && dataType != TSDataType.TIMESTAMP) {
        return peerGroupEnd;
      }

      int recentEnd = recentRange.getEnd();

      // Leave section of leading nulls
      if (frameInfo.getSortOrder().isNullsFirst() && column.isNull(recentEnd)) {
        // Then the frame starts with current row
        recentEnd = index;
      }

      if (frameInfo.getSortOrder().isAscending()) {
        offset = getAscFrameEndFollowing(index, recentEnd);
      } else {
        offset = getDescFrameEndFollowing(index, recentEnd);
      }
    }

    return offset;
  }

  // Find first row which satisfy:
  // follow >= current + offset
  // And stop right there
  private int getAscFrameStartFollowing(int currentIndex, int recentIndex) {
    while (recentIndex < partitionSize && !column.isNull(recentIndex)) {
      if (compareInAscFrameStartFollowing(
          currentIndex, recentIndex, frameInfo.getStartOffsetChannel())) {
        return recentIndex;
      }
      recentIndex++;
    }
    return recentIndex;
  }

  private boolean compareInAscFrameStartFollowing(int currentIndex, int recentIndex, int channel) {
    checkArgument(!partition.isNull(channel, currentIndex));
    switch (column.getDataType()) {
      case INT32:
      case DATE:
        int currentInt = column.getInt(currentIndex);
        int followInt = column.getInt(recentIndex);
        int deltaInt = partition.getInt(channel, currentIndex);
        return followInt >= currentInt + deltaInt;
      case INT64:
      case TIMESTAMP:
        long currentLong = column.getLong(currentIndex);
        long followLong = column.getLong(recentIndex);
        long deltaLong = partition.getLong(channel, currentIndex);
        return followLong >= currentLong + deltaLong;
      case FLOAT:
        float currentFloat = column.getFloat(currentIndex);
        float followFloat = column.getFloat(recentIndex);
        float deltaFloat = partition.getFloat(channel, currentIndex);
        return followFloat >= currentFloat + deltaFloat;
      case DOUBLE:
        double currentDouble = column.getDouble(currentIndex);
        double followDouble = column.getDouble(recentIndex);
        double deltaDouble = partition.getDouble(channel, currentIndex);
        return followDouble >= currentDouble + deltaDouble;
      default:
        // Unreachable
        throw new UnSupportedDataTypeException("Unsupported data type: " + column.getDataType());
    }
  }

  // Find first row which satisfy:
  // follow > current + offset
  // And return its previous index
  private int getAscFrameEndFollowing(int currentIndex, int recentIndex) {
    while (recentIndex < partitionSize && !column.isNull(recentIndex)) {
      if (compareInAscFrameEndFollowing(
          currentIndex, recentIndex, frameInfo.getEndOffsetChannel())) {
        return recentIndex - 1;
      }
      recentIndex++;
    }
    return recentIndex - 1;
  }

  private boolean compareInAscFrameEndFollowing(int currentIndex, int recentIndex, int channel) {
    checkArgument(!partition.isNull(channel, currentIndex));
    switch (column.getDataType()) {
      case INT32:
      case DATE:
        int currentInt = column.getInt(currentIndex);
        int followInt = column.getInt(recentIndex);
        int deltaInt = partition.getInt(channel, currentIndex);
        return followInt > currentInt + deltaInt;
      case INT64:
      case TIMESTAMP:
        long currentLong = column.getLong(currentIndex);
        long followLong = column.getLong(recentIndex);
        long deltaLong = partition.getLong(channel, currentIndex);
        return followLong > currentLong + deltaLong;
      case FLOAT:
        float currentFloat = column.getFloat(currentIndex);
        float followFloat = column.getFloat(recentIndex);
        float deltaFloat = partition.getFloat(channel, currentIndex);
        return followFloat > currentFloat + deltaFloat;
      case DOUBLE:
        double currentDouble = column.getDouble(currentIndex);
        double followDouble = column.getDouble(recentIndex);
        double deltaDouble = partition.getDouble(channel, currentIndex);
        return followDouble > currentDouble + deltaDouble;
      default:
        // Unreachable
        throw new UnSupportedDataTypeException("Unsupported data type: " + column.getDataType());
    }
  }

  // Find first row which satisfy:
  // precede >= current - offset
  // And stop right there
  private int getAscFrameStartPreceding(int currentIndex, int recentIndex) {
    while (recentIndex < currentIndex) {
      if (compareInAscFrameStartPreceding(
          currentIndex, recentIndex, frameInfo.getStartOffsetChannel())) {
        return recentIndex;
      }
      recentIndex++;
    }
    return recentIndex;
  }

  private boolean compareInAscFrameStartPreceding(int currentIndex, int recentIndex, int channel) {
    checkArgument(!partition.isNull(channel, currentIndex));
    switch (column.getDataType()) {
      case INT32:
      case DATE:
        int currentInt = column.getInt(currentIndex);
        int precedeInt = column.getInt(recentIndex);
        int deltaInt = partition.getInt(channel, currentIndex);
        return precedeInt >= currentInt - deltaInt;
      case INT64:
      case TIMESTAMP:
        long currentLong = column.getLong(currentIndex);
        long precedeLong = column.getLong(recentIndex);
        long deltaLong = partition.getLong(channel, currentIndex);
        return precedeLong >= currentLong - deltaLong;
      case FLOAT:
        float currentFloat = column.getFloat(currentIndex);
        float precedeFollow = column.getFloat(recentIndex);
        float deltaFloat = partition.getFloat(channel, currentIndex);
        return precedeFollow >= currentFloat - deltaFloat;
      case DOUBLE:
        double currentDouble = column.getDouble(currentIndex);
        double precedeDouble = column.getDouble(recentIndex);
        double deltaDouble = partition.getDouble(channel, currentIndex);
        return precedeDouble >= currentDouble - deltaDouble;
      default:
        // Unreachable
        throw new UnSupportedDataTypeException("Unsupported data type: " + column.getDataType());
    }
  }

  // Find first row which satisfy:
  // precede > current - offset
  // And return its previous index
  private int getAscFrameEndPreceding(int currentIndex, int recentIndex) {
    while (recentIndex < partitionSize) {
      if (compareInAscFrameEndPreceding(
          currentIndex, recentIndex, frameInfo.getEndOffsetChannel())) {
        return recentIndex - 1;
      }
      recentIndex++;
    }
    return recentIndex - 1;
  }

  private boolean compareInAscFrameEndPreceding(int currentIndex, int recentIndex, int channel) {
    checkArgument(!partition.isNull(channel, currentIndex));
    switch (column.getDataType()) {
      case INT32:
      case DATE:
        int currentInt = column.getInt(currentIndex);
        int precedeInt = column.getInt(recentIndex);
        int deltaInt = partition.getInt(channel, currentIndex);
        return precedeInt > currentInt - deltaInt;
      case INT64:
      case TIMESTAMP:
        long currentLong = column.getLong(currentIndex);
        long precedeLong = column.getLong(recentIndex);
        long deltaLong = partition.getLong(channel, currentIndex);
        return precedeLong > currentLong - deltaLong;
      case FLOAT:
        float currentFloat = column.getFloat(currentIndex);
        float precedeFollow = column.getFloat(recentIndex);
        float deltaFloat = partition.getFloat(channel, currentIndex);
        return precedeFollow > currentFloat - deltaFloat;
      case DOUBLE:
        double currentDouble = column.getDouble(currentIndex);
        double precedeDouble = column.getDouble(recentIndex);
        double deltaDouble = partition.getDouble(channel, currentIndex);
        return precedeDouble > currentDouble - deltaDouble;
      default:
        // Unreachable
        throw new UnSupportedDataTypeException("Unsupported data type: " + column.getDataType());
    }
  }

  // Find first row which satisfy:
  // follow <= current - offset
  // And stop right there
  private int getDescFrameStartFollowing(int currentIndex, int recentIndex) {
    while (recentIndex < partitionSize && !column.isNull(recentIndex)) {
      if (compareInDescFrameStartFollowing(
          currentIndex, recentIndex, frameInfo.getStartOffsetChannel())) {
        return recentIndex;
      }
      recentIndex++;
    }
    return recentIndex;
  }

  private boolean compareInDescFrameStartFollowing(int currentIndex, int recentIndex, int channel) {
    checkArgument(!partition.isNull(channel, currentIndex));
    switch (column.getDataType()) {
      case INT32:
      case DATE:
        int currentInt = column.getInt(currentIndex);
        int followInt = column.getInt(recentIndex);
        int deltaInt = partition.getInt(channel, currentIndex);
        return followInt <= currentInt - deltaInt;
      case INT64:
      case TIMESTAMP:
        long currentLong = column.getLong(currentIndex);
        long followLong = column.getLong(recentIndex);
        long deltaLong = partition.getLong(channel, currentIndex);
        return followLong <= currentLong - deltaLong;
      case FLOAT:
        float currentFloat = column.getFloat(currentIndex);
        float followFloat = column.getFloat(recentIndex);
        float deltaFloat = partition.getFloat(channel, currentIndex);
        return followFloat <= currentFloat - deltaFloat;
      case DOUBLE:
        double currentDouble = column.getDouble(currentIndex);
        double followDouble = column.getDouble(recentIndex);
        double deltaDouble = partition.getDouble(channel, currentIndex);
        return followDouble <= currentDouble - deltaDouble;
      default:
        // Unreachable
        throw new UnSupportedDataTypeException("Unsupported data type: " + column.getDataType());
    }
  }

  // Find first row which satisfy:
  // follow < current - offset
  // And return its previous index
  private int getDescFrameEndFollowing(int currentIndex, int recentIndex) {
    while (recentIndex < partitionSize && !column.isNull(recentIndex)) {
      if (compareInDescFrameEndFollowing(
          currentIndex, recentIndex, frameInfo.getEndOffsetChannel())) {
        return recentIndex - 1;
      }
      recentIndex++;
    }
    return recentIndex - 1;
  }

  private boolean compareInDescFrameEndFollowing(int currentIndex, int recentIndex, int channel) {
    checkArgument(!partition.isNull(channel, currentIndex));
    switch (column.getDataType()) {
      case INT32:
      case DATE:
        int currentInt = column.getInt(currentIndex);
        int followInt = column.getInt(recentIndex);
        int deltaInt = partition.getInt(channel, currentIndex);
        return followInt < currentInt - deltaInt;
      case INT64:
      case TIMESTAMP:
        long currentLong = column.getLong(currentIndex);
        long followLong = column.getLong(recentIndex);
        long deltaLong = partition.getLong(channel, currentIndex);
        return followLong < currentLong - deltaLong;
      case FLOAT:
        float currentFloat = column.getFloat(currentIndex);
        float followFloat = column.getFloat(recentIndex);
        float deltaFloat = partition.getFloat(channel, currentIndex);
        return followFloat < currentFloat - deltaFloat;
      case DOUBLE:
        double currentDouble = column.getDouble(currentIndex);
        double followDouble = column.getDouble(recentIndex);
        double deltaDouble = partition.getDouble(channel, currentIndex);
        return followDouble < currentDouble - deltaDouble;
      default:
        // Unreachable
        throw new UnSupportedDataTypeException("Unsupported data type: " + column.getDataType());
    }
  }

  // Find first row which satisfy:
  // precede <= current + offset
  // And stop right there
  private int getDescFrameStartPreceding(int currentIndex, int recentIndex) {
    while (recentIndex < currentIndex) {
      if (compareInDescFrameStartPreceding(
          currentIndex, recentIndex, frameInfo.getStartOffsetChannel())) {
        return recentIndex;
      }
      recentIndex++;
    }
    return recentIndex;
  }

  private boolean compareInDescFrameStartPreceding(int currentIndex, int recentIndex, int channel) {
    checkArgument(!partition.isNull(channel, currentIndex));
    switch (column.getDataType()) {
      case INT32:
      case DATE:
        int currentInt = column.getInt(currentIndex);
        int precedeInt = column.getInt(recentIndex);
        int deltaInt = partition.getInt(channel, currentIndex);
        return precedeInt <= currentInt + deltaInt;
      case INT64:
      case TIMESTAMP:
        long currentLong = column.getLong(currentIndex);
        long precedeLong = column.getLong(recentIndex);
        long deltaLong = partition.getLong(channel, currentIndex);
        return precedeLong <= currentLong + deltaLong;
      case FLOAT:
        float currentFloat = column.getFloat(currentIndex);
        float precedeFollow = column.getFloat(recentIndex);
        float deltaFloat = partition.getFloat(channel, currentIndex);
        return precedeFollow <= currentFloat + deltaFloat;
      case DOUBLE:
        double currentDouble = column.getDouble(currentIndex);
        double precedeDouble = column.getDouble(recentIndex);
        double deltaDouble = partition.getDouble(channel, currentIndex);
        return precedeDouble <= currentDouble + deltaDouble;
      default:
        // Unreachable
        throw new UnSupportedDataTypeException("Unsupported data type: " + column.getDataType());
    }
  }

  // Find first row which satisfy:
  // precede < current + offset
  // And return its previous index
  private int getDescFrameEndPreceding(int currentIndex, int recentIndex) {
    while (recentIndex < partitionSize) {
      if (compareInDescFrameEndPreceding(
          currentIndex, recentIndex, frameInfo.getEndOffsetChannel())) {
        return recentIndex - 1;
      }
      recentIndex++;
    }
    return recentIndex - 1;
  }

  private boolean compareInDescFrameEndPreceding(int currentIndex, int recentIndex, int channel) {
    checkArgument(!partition.isNull(channel, currentIndex));
    switch (column.getDataType()) {
      case INT32:
      case DATE:
        int currentInt = column.getInt(currentIndex);
        int precedeInt = column.getInt(recentIndex);
        int deltaInt = partition.getInt(channel, currentIndex);
        return precedeInt < currentInt + deltaInt;
      case INT64:
      case TIMESTAMP:
        long currentLong = column.getLong(currentIndex);
        long precedeLong = column.getLong(recentIndex);
        long deltaLong = partition.getLong(channel, currentIndex);
        return precedeLong < currentLong + deltaLong;
      case FLOAT:
        float currentFloat = column.getFloat(currentIndex);
        float precedeFollow = column.getFloat(recentIndex);
        float deltaFloat = partition.getFloat(channel, currentIndex);
        return precedeFollow < currentFloat + deltaFloat;
      case DOUBLE:
        double currentDouble = column.getDouble(currentIndex);
        double precedeDouble = column.getDouble(recentIndex);
        double deltaDouble = partition.getDouble(channel, currentIndex);
        return precedeDouble < currentDouble + deltaDouble;
      default:
        // Unreachable
        throw new UnSupportedDataTypeException("Unsupported data type: " + column.getDataType());
    }
  }
}
