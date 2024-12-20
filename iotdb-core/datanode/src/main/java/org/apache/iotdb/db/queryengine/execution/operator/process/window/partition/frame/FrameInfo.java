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
  // By convention, if a value can be an integer or a floating point
  // Then it is stored in double
  private final double startOffset; // For PRECEDING and FOLLOWING use
  private final FrameBoundType endType;
  private final double endOffset; // Same as startOffset
  // For RANGE type frame
  private final int sortChannel;
  private final SortOrder sortOrder;

  public FrameInfo(FrameType frameType, FrameBoundType startType, FrameBoundType endType) {
    this(frameType, startType, -1, endType, -1);
  }

  public FrameInfo(
      FrameType frameType, FrameBoundType startType, int startOffset, FrameBoundType endType) {
    this(frameType, startType, startOffset, endType, -1);
  }

  public FrameInfo(
      FrameType frameType,
      FrameBoundType startType,
      int startOffset,
      FrameBoundType endType,
      int sortChannel,
      SortOrder sortOrder) {
    this(frameType, startType, startOffset, endType, -1, sortChannel, sortOrder);
  }

  public FrameInfo(
      FrameType frameType, FrameBoundType startType, FrameBoundType endType, int endOffset) {
    this(frameType, startType, -1, endType, endOffset);
  }

  public FrameInfo(
      FrameType frameType,
      FrameBoundType startType,
      FrameBoundType endType,
      int endOffset,
      int sortChannel,
      SortOrder sortOrder) {
    this(frameType, startType, -1, endType, endOffset, sortChannel, sortOrder);
  }

  public FrameInfo(
      FrameType frameType,
      FrameBoundType startType,
      int startOffset,
      FrameBoundType endType,
      int endOffset) {
    this(frameType, startType, startOffset, endType, endOffset, -1, SortOrder.ASC_NULLS_FIRST);
  }

  public FrameInfo(
      FrameType frameType,
      FrameBoundType startType,
      int startOffset,
      FrameBoundType endType,
      int endOffset,
      int sortChannel,
      SortOrder sortOrder) {
    this.frameType = frameType;
    this.startType = startType;
    this.startOffset = startOffset;
    this.endType = endType;
    this.endOffset = endOffset;
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

  public double getStartOffset() {
    return startOffset;
  }

  public double getEndOffset() {
    return endOffset;
  }

  public int getSortChannel() {
    return sortChannel;
  }

  public SortOrder getSortOrder() {
    return sortOrder;
  }
}
