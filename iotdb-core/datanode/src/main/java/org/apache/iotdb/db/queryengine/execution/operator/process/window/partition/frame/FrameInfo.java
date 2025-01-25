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

import org.apache.iotdb.db.queryengine.plan.relational.planner.SortOrder;

public class FrameInfo {
  public enum FrameType {
    RANGE,
    ROWS,
    GROUPS
  }

  public enum FrameBoundType {
    UNBOUNDED_PRECEDING,
    PRECEDING,
    CURRENT_ROW,
    FOLLOWING,
    UNBOUNDED_FOLLOWING;
  }

  private final FrameType frameType;
  private final FrameBoundType startType;
  private final int startOffsetChannel; // For PRECEDING and FOLLOWING use
  private final FrameBoundType endType;
  private final int endOffsetChannel; // Same as startOffset
  // For RANGE type frame
  private final int sortChannel;
  private final SortOrder sortOrder;

  public FrameInfo(FrameType frameType, FrameBoundType startType, FrameBoundType endType) {
    this(frameType, startType, -1, endType, -1);
  }

  public FrameInfo(
      FrameType frameType,
      FrameBoundType startType,
      int startOffsetChannel,
      FrameBoundType endType) {
    this(frameType, startType, startOffsetChannel, endType, -1);
  }

  public FrameInfo(
      FrameType frameType,
      FrameBoundType startType,
      int startOffsetChannel,
      FrameBoundType endType,
      int sortChannel,
      SortOrder sortOrder) {
    this(frameType, startType, startOffsetChannel, endType, -1, sortChannel, sortOrder);
  }

  public FrameInfo(
      FrameType frameType, FrameBoundType startType, FrameBoundType endType, int endOffsetChannel) {
    this(frameType, startType, -1, endType, endOffsetChannel);
  }

  public FrameInfo(
      FrameType frameType,
      FrameBoundType startType,
      FrameBoundType endType,
      int endOffsetChannel,
      int sortChannel,
      SortOrder sortOrder) {
    this(frameType, startType, -1, endType, endOffsetChannel, sortChannel, sortOrder);
  }

  public FrameInfo(
      FrameType frameType,
      FrameBoundType startType,
      int startOffsetChannel,
      FrameBoundType endType,
      int endOffsetChannel) {
    this(
        frameType,
        startType,
        startOffsetChannel,
        endType,
        endOffsetChannel,
        -1,
        SortOrder.ASC_NULLS_FIRST);
  }

  public FrameInfo(
      FrameType frameType,
      FrameBoundType startType,
      int startOffsetChannel,
      FrameBoundType endType,
      int endOffsetChannel,
      int sortChannel,
      SortOrder sortOrder) {
    this.frameType = frameType;
    this.startType = startType;
    this.startOffsetChannel = startOffsetChannel;
    this.endType = endType;
    this.endOffsetChannel = endOffsetChannel;
    this.sortChannel = sortChannel;
    this.sortOrder = sortOrder;
  }

  public FrameType getFrameType() {
    return frameType;
  }

  public FrameBoundType getStartType() {
    return startType;
  }

  public FrameBoundType getEndType() {
    return endType;
  }

  public int getStartOffsetChannel() {
    return startOffsetChannel;
  }

  public int getEndOffsetChannel() {
    return endOffsetChannel;
  }

  public int getSortChannel() {
    return sortChannel;
  }

  public SortOrder getSortOrder() {
    return sortOrder;
  }
}
