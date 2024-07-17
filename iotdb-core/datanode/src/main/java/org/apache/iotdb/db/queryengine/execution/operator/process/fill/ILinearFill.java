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

package org.apache.iotdb.db.queryengine.execution.operator.process.fill;

import org.apache.tsfile.block.column.Column;

public interface ILinearFill {

  /**
   * Before we call this method, we need to make sure the nextValue has been prepared or noMoreNext
   * has been set to true.
   *
   * @param timeColumn TimeColumn of valueColumn
   * @param valueColumn valueColumn that need to be filled
   * @param currentRowIndex current row index for start time in timeColumn
   * @return Value Column that has been filled
   */
  Column fill(Column timeColumn, Column valueColumn, long currentRowIndex);

  /**
   * Whether need prepare for next.
   *
   * @param rowIndex row index for end time of current valueColumn that need to be filled
   * @param valueColumn valueColumn that need to be filled
   * @return true if valueColumn can't be filled using current information, and we need to get next
   *     TsBlock and then call prepareForNext. false if valueColumn can be filled using current
   *     information, and we can directly call fill() function
   */
  boolean needPrepareForNext(long rowIndex, Column valueColumn);

  /**
   * prepare for next.
   *
   * @param startRowIndex row index for start time of nextValueColumn
   * @param endRowIndex row index for end time of current valueColumn that need to be filled
   * @param nextTimeColumn TimeColumn of next TsBlock
   * @param nextValueColumn Value Column of next TsBlock
   * @return true if we get enough information to fill current column, and we can stop getting next
   *     TsBlock and calling prepareForNext. false if we still don't get enough information to fill
   *     current column, and still need to keep getting next TsBlock and then call prepareForNext
   */
  boolean prepareForNext(
      long startRowIndex, long endRowIndex, Column nextTimeColumn, Column nextValueColumn);
}
