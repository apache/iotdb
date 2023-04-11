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

public interface RawDataWindowManager {

  /** Check whether current window is initialized. */
  boolean isCurWindowInit();

  /** Check if the data in current window is enough, if so, the window can be moved to next. */
  boolean isCurWindowFinished();

  /** Move to next window, update some status values. */
  void nextWindow();

  /** if there is more window, or if the data in last window is output. */
  boolean hasNext();

  /** Initialize current window. It will be revoked after nextWindow(). */
  TsBlock initWindow(TsBlock tsBlock);

  /**
   * process tsBlock in current window, which will update the status in window and record the
   * tsBlock in tsBlockBuilder.
   *
   * @param tsBlock the tsBlock to be processed in current window, one window may have multi
   *     tsBlocks to process.
   * @return the index of the last row in tsBlock to process, if the whole tsBlock is consumed up,
   *     return the positionCount of tsBlock.
   */
  int process(TsBlock tsBlock);

  /** @return the tsBlock recorded in tsBlockBuilder during process() */
  TsBlock buildTsBlock();
}
