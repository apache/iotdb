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
import org.apache.iotdb.db.queryengine.execution.operator.process.window.utils.Range;

import static com.google.common.base.Preconditions.checkArgument;

public class RowsFrame implements Frame {
  private final Partition partition;
  private final FrameInfo frameInfo;
  private final int partitionSize;

  public RowsFrame(Partition partition, FrameInfo frameInfo) {
    checkArgument(frameInfo.getFrameType() == FrameInfo.FrameType.ROWS);

    this.partition = partition;
    this.frameInfo = frameInfo;
    this.partitionSize = partition.getPositionCount();
  }

  @Override
  public Range getRange(
      int currentPosition, int currentGroup, int peerGroupStart, int peerGroupEnd) {
    int offset;
    int frameStart;
    switch (frameInfo.getStartType()) {
      case UNBOUNDED_PRECEDING:
        frameStart = 0;
        break;
      case PRECEDING:
        offset = (int) getOffset(frameInfo.getStartOffsetChannel(), currentPosition);
        frameStart = currentPosition - offset;
        break;
      case CURRENT_ROW:
        frameStart = currentPosition;
        break;
      case FOLLOWING:
        offset = (int) getOffset(frameInfo.getStartOffsetChannel(), currentPosition);
        frameStart = currentPosition + offset;
        break;
      default:
        // UNBOUND_FOLLOWING is not allowed in frame start
        throw new SemanticException("UNBOUND PRECEDING is not allowed in frame start!");
    }

    int frameEnd;
    switch (frameInfo.getEndType()) {
      case PRECEDING:
        offset = (int) getOffset(frameInfo.getEndOffsetChannel(), currentPosition);
        frameEnd = currentPosition - offset;
        break;
      case CURRENT_ROW:
        frameEnd = currentPosition;
        break;
      case FOLLOWING:
        offset = (int) getOffset(frameInfo.getEndOffsetChannel(), currentPosition);
        frameEnd = currentPosition + offset;
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
    return new Range(frameStart, frameEnd);
  }

  public long getOffset(int channel, int index) {
    checkArgument(!partition.isNull(channel, index));
    long offset = partition.getLong(channel, index);

    checkArgument(offset >= 0);
    return offset;
  }
}
