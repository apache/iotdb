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

package org.apache.iotdb.udf.api.access;

import org.apache.iotdb.udf.api.customizer.strategy.SlidingSizeWindowAccessStrategy;
import org.apache.iotdb.udf.api.customizer.strategy.SlidingTimeWindowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

import java.io.IOException;

public interface RowWindow {

  /**
   * Returns the number of rows in this window.
   *
   * @return the number of rows in this window
   */
  int windowSize();

  /**
   * Returns the row at the specified position in this window.
   *
   * <p>Note that the Row instance returned by this method each time is the same instance. In other
   * words, calling this method will only change the member variables inside the Row instance, but
   * will not generate a new Row instance.
   *
   * @param rowIndex index of the row to return
   * @return the row at the specified position in this window, throw IndexOutOfBoundException if
   *     call this method on an empty RowWindow.
   * @throws IOException if any I/O errors occur
   */
  Row getRow(int rowIndex) throws IOException;

  /**
   * Returns the actual data type of the values at the specified column in this window.
   *
   * @param columnIndex index of the specified column
   * @return the actual data type of the values at the specified column in this window
   */
  Type getDataType(int columnIndex);

  /**
   * Returns an iterator used to access this window.
   *
   * @return an iterator used to access this window
   */
  RowIterator getRowIterator();

  /**
   * For different types of windows, the definition of the window start time is different.
   *
   * <p>For sliding size window: the window start time is equal to the timestamp of the first row.
   *
   * <p>For sliding time window: The window start time is determined by displayWindowBegin {@link
   * SlidingTimeWindowAccessStrategy#getDisplayWindowBegin()} and slidingStep {@link
   * SlidingTimeWindowAccessStrategy#getSlidingStep()}. <br>
   * The window start time for the i-th window (i starts at 0) can be calculated as {@code
   * displayWindowBegin + i * slidingStep}.
   *
   * @return the start time of the window
   * @since 0.13.0
   * @see SlidingSizeWindowAccessStrategy
   * @see SlidingTimeWindowAccessStrategy
   */
  long windowStartTime();

  /**
   * For different types of windows, the definition of the window end time is different.
   *
   * <p>For sliding size window: the window end time is equal to the timestamp of the last row.
   *
   * <p>For sliding time window: The window end time is determined by displayWindowBegin {@link
   * SlidingTimeWindowAccessStrategy#getDisplayWindowBegin()}, timeInterval {@link
   * SlidingTimeWindowAccessStrategy#getTimeInterval()} and slidingStep {@link
   * SlidingTimeWindowAccessStrategy#getSlidingStep()}. <br>
   * The window end time for the i-th window (i starts at 0) can be calculated as {@code
   * displayWindowBegin + i * slidingStep + timeInterval - 1} or {@code windowStartTime(i) +
   * timeInterval - 1}.
   *
   * @return the end time of the window
   * @since 0.13.0
   * @see SlidingSizeWindowAccessStrategy
   * @see SlidingTimeWindowAccessStrategy
   */
  long windowEndTime();
}
