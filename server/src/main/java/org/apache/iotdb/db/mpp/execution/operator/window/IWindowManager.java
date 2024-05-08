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

package org.apache.iotdb.db.mpp.execution.operator.window;

import org.apache.iotdb.tsfile.read.common.block.TsBlock;

/**
 * Used to customize all the type of window managers, such as TimeWindowManager,
 * SessionWindowManager
 */
public interface IWindowManager {

  /**
   * Judge whether the current window is initialized
   *
   * @return whether the current window is initialized
   */
  boolean isCurWindowInit();

  /**
   * Used to initialize the status of window
   *
   * @param tsBlock a TsBlock
   */
  void initCurWindow(TsBlock tsBlock);

  /**
   * Used to determine whether there is a next window
   *
   * @return whether there is a next window
   */
  boolean hasNext(boolean hasMoreData);

  /** Used to mark the current window has got last point */
  void next();

  /**
   * Used to get the output time of current window
   *
   * @return the output time of current window
   */
  long currentOutputTime();

  /**
   * Used to get current window
   *
   * @return current window
   */
  IWindow getCurWindow();

  /**
   * Used to skip some points through the window attributes, such as timeRange and so on.
   *
   * @param inputTsBlock a TsBlock
   * @return a new TsBlock which skips some points
   */
  TsBlock skipPointsOutOfCurWindow(TsBlock inputTsBlock);

  /**
   * Used to determine whether the current window overlaps with TsBlock
   *
   * @param inputTsBlock a TsBlock
   * @return whether the current window overlaps with TsBlock
   */
  boolean satisfiedCurWindow(TsBlock inputTsBlock);

  /**
   * Used to determine whether there are extra points for the next window
   *
   * @param inputTsBlock a TsBlock
   * @return whether there are extra points for the next window
   */
  boolean isTsBlockOutOfBound(TsBlock inputTsBlock);
}
