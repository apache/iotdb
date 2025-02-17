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

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public class GroupsFrame implements Frame {
  private final Partition partition;
  private final FrameInfo frameInfo;
  private final int partitionSize;

  private final List<ColumnList> columns;
  private final RowComparator peerGroupComparator;

  private Range recentRange;
  private int recentStartPeerGroup;
  private int recentEndPeerGroup;
  private boolean frameStartFollowingReachEnd = false;

  public GroupsFrame(
      Partition partition,
      FrameInfo frameInfo,
      List<ColumnList> sortedColumns,
      RowComparator peerGroupComparator,
      int initialEnd) {
    this.partition = partition;
    this.frameInfo = frameInfo;
    this.partitionSize = partition.getPositionCount();
    this.columns = sortedColumns;
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
        frameStart = getStartPrecedingOffset(currentPosition, currentGroup);
        break;
      case CURRENT_ROW:
        frameStart = peerGroupStart;
        break;
      case FOLLOWING:
        frameStart = getStartFollowingOffset(currentPosition, currentGroup);
        break;
      default:
        // UNBOUND_FOLLOWING is not allowed in frame start
        throw new SemanticException("UNBOUND FOLLOWING is not allowed in frame start!");
    }

    int frameEnd;
    switch (frameInfo.getEndType()) {
      case PRECEDING:
        frameEnd = getEndPrecedingOffset(currentPosition, currentGroup);
        break;
      case CURRENT_ROW:
        frameEnd = peerGroupEnd - 1;
        break;
      case FOLLOWING:
        frameEnd = getEndFollowingOffset(currentPosition, currentGroup);
        break;
      case UNBOUNDED_FOLLOWING:
        frameEnd = partitionSize - 1;
        break;
      default:
        // UNBOUND_PRECEDING is not allowed in frame end
        throw new SemanticException("UNBOUND PRECEDING is not allowed in frame end!");
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

  private int getStartPrecedingOffset(int currentPosition, int currentGroup) {
    int start = recentRange.getStart();
    int offset = (int) getOffset(frameInfo.getStartOffsetChannel(), currentPosition);

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

    return start;
  }

  private int getEndPrecedingOffset(int currentPosition, int currentGroup) {
    int end = recentRange.getEnd();
    int offset = (int) getOffset(frameInfo.getEndOffsetChannel(), currentPosition);

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

    return end;
  }

  private int getStartFollowingOffset(int currentPosition, int currentGroup) {
    // Shortcut if we have reached last peer group already
    if (frameStartFollowingReachEnd) {
      return partitionSize;
    }

    int start = recentRange.getStart();

    int offset = (int) getOffset(frameInfo.getStartOffsetChannel(), currentPosition);
    if (currentGroup + offset > recentStartPeerGroup) {
      int count = currentGroup + offset - recentStartPeerGroup;
      for (int i = 0; i < count; i++) {
        // Scan over current peer group
        start = scanPeerGroup(start);
        // Enter next peer group
        if (start == partitionSize - 1) {
          // Reach partition end
          // We may encounter empty frame here
          recentStartPeerGroup = currentGroup + i;
          frameStartFollowingReachEnd = true;
          return partitionSize;
        } else {
          start++;
        }
      }
      recentStartPeerGroup = currentGroup + offset;
    }

    return start;
  }

  private int getEndFollowingOffset(int currentPosition, int currentGroup) {
    int end = recentRange.getEnd();
    // Shortcut if we have reached partition end already
    if (end == partitionSize - 1) {
      return end;
    }

    int offset = (int) getOffset(frameInfo.getEndOffsetChannel(), currentPosition);
    if (currentGroup + offset > recentEndPeerGroup) {
      int count = currentGroup + offset - recentEndPeerGroup;
      for (int i = 0; i < count; i++) {
        // Enter next peer group
        if (end == partitionSize - 1) {
          if (i != count - 1) {
            // Too early, we may encounter empty frame
            return partitionSize;
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

    return end;
  }

  private int scanPeerGroup(int currentPosition) {
    while (currentPosition < partitionSize - 1
        && peerGroupComparator.equalColumnLists(columns, currentPosition, currentPosition + 1)) {
      currentPosition++;
    }
    return currentPosition;
  }

  public long getOffset(int channel, int index) {
    checkArgument(!partition.isNull(channel, index));
    long offset = partition.getLong(channel, index);

    checkArgument(offset >= 0);
    return offset;
  }
}
