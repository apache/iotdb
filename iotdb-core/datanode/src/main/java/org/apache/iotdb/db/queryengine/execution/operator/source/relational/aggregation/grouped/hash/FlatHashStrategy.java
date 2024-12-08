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

package org.apache.iotdb.db.queryengine.execution.operator.source.relational.aggregation.grouped.hash;

import org.apache.tsfile.block.column.Column;
import org.apache.tsfile.block.column.ColumnBuilder;

public interface FlatHashStrategy {
  boolean isAnyVariableWidth();

  int getTotalFlatFixedLength();

  int getTotalVariableWidth(Column[] columns, int position);

  void readFlat(
      byte[] fixedChunk, int fixedOffset, byte[] variableChunk, ColumnBuilder[] columnBuilders);

  void writeFlat(
      Column[] columns,
      int position,
      byte[] fixedChunk,
      int fixedOffset,
      byte[] variableChunk,
      int variableOffset);

  boolean valueNotDistinctFrom(
      byte[] leftFixedChunk,
      int leftFixedOffset,
      byte[] leftVariableChunk,
      Column[] rightColumns,
      int rightPosition);

  long hash(Column[] columns, int position);

  long hash(byte[] fixedChunk, int fixedOffset, byte[] variableChunk);

  void hashBatched(Column[] columns, long[] hashes, int offset, int length);
}
