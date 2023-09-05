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

package org.apache.iotdb.db.queryengine.execution.operator.window;

import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.Column;

/** Used to customize the window which stipulates where we can calculate aggregation result. */
public interface IWindow {

  /**
   * Get control column.
   *
   * @return the control column
   */
  Column getControlColumn(TsBlock tsBlock);

  /**
   * Judge whether the point at index of column belongs to this window.
   *
   * @param column the controlColumn of window
   * @param index the row index in column
   * @return if the indexed row of column satisfy the window
   */
  boolean satisfy(Column column, int index);

  /**
   * When we merge a point into window, at this time, we can use this method to change the status in
   * this window.
   */
  void mergeOnePoint(Column[] timeAndValueColumn, int index);

  /**
   * Used to judge whether the window has contains the column.
   *
   * @param column the controlColumn of window
   * @return if the whole column satisfy the window
   */
  boolean contains(Column column);
}
