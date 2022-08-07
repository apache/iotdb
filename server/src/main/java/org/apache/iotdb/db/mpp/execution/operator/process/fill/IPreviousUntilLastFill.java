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
package org.apache.iotdb.db.mpp.execution.operator.process.fill;

import org.apache.iotdb.tsfile.read.common.block.column.Column;

public interface IPreviousUntilLastFill {

  Column fill(Column valueColumn, int index);

  /**
   * Check if the column needs the next tsblock.
   *
   * @return true If the current tsblock has a value in this column and the last row has no value.
   *     If the previous tsblock has a value, the middle tsblock is all null, and after the index of
   *     the lastHasValueTsBlockIndex . false If the current column is all null, continues to be
   *     null from the first tsblock and subsequent tsblocks. If the previous tsblock has a value,
   *     and the middle tsblock appears all null, and before the index of the
   *     lastHasValueTsBlockIndex. If the last value of the current tsblock is not null
   */
  boolean needPrepareForNext(Column valueColumn, int currentIndex);

  boolean allValueIsNull(Column valueColumn);

  void updateLastHasValueTsBlockIndex(int lastHasValueTsBlockIndex);
}
