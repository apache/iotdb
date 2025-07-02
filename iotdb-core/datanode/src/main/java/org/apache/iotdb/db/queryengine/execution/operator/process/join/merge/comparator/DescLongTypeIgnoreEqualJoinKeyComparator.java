/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.comparator;

import org.apache.tsfile.read.common.block.TsBlock;

import java.util.Optional;

// This Comparator is used to handle the case where the join condition is l<=r, making its interface
// consistent with the case that l<r, such unity helps to avoid branch judgments during the process
// of operator.
public class DescLongTypeIgnoreEqualJoinKeyComparator implements JoinKeyComparator {

  private static final DescLongTypeIgnoreEqualJoinKeyComparator INSTANCE =
      new DescLongTypeIgnoreEqualJoinKeyComparator();

  private DescLongTypeIgnoreEqualJoinKeyComparator() {
    // hide constructor
  }

  public static DescLongTypeIgnoreEqualJoinKeyComparator getInstance() {
    return INSTANCE;
  }

  @Override
  public Optional<Boolean> lessThan(
      TsBlock left,
      int leftColumnIndex,
      int leftRowIndex,
      TsBlock right,
      int rightColumnIndex,
      int rightRowIndex) {
    if (left.getColumn(leftColumnIndex).isNull(leftRowIndex)
        || right.getColumn(rightColumnIndex).isNull(rightRowIndex)) {
      return Optional.empty();
    }

    return Optional.of(
        left.getColumn(leftColumnIndex).getLong(leftRowIndex)
            >= right.getColumn(rightColumnIndex).getLong(rightRowIndex));
  }

  @Override
  public Optional<Boolean> equalsTo(
      TsBlock left,
      int leftColumnIndex,
      int leftRowIndex,
      TsBlock right,
      int rightColumnIndex,
      int rightRowIndex) {
    if (left.getColumn(leftColumnIndex).isNull(leftRowIndex)
        || right.getColumn(rightColumnIndex).isNull(rightRowIndex)) {
      return Optional.empty();
    }

    return Optional.of(
        left.getColumn(leftColumnIndex).getLong(leftRowIndex)
            == right.getColumn(rightColumnIndex).getLong(rightRowIndex));
  }

  @Override
  public Optional<Boolean> lessThanOrEqual(
      TsBlock left,
      int leftColumnIndex,
      int leftRowIndex,
      TsBlock right,
      int rightColumnIndex,
      int rightRowIndex) {
    if (left.getColumn(leftColumnIndex).isNull(leftRowIndex)
        || right.getColumn(rightColumnIndex).isNull(rightRowIndex)) {
      return Optional.empty();
    }

    return Optional.of(
        left.getColumn(leftColumnIndex).getLong(leftRowIndex)
            > right.getColumn(rightColumnIndex).getLong(rightRowIndex));
  }
}
