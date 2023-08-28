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

package org.apache.iotdb.db.utils.datastructure;

import org.apache.iotdb.tsfile.read.common.block.TsBlock;

public class MergeSortKey extends SortKey {

  // This filed only used in operation as a intermediate tool, which is used to locate the origin of
  // sortKey when there are more than one channels as input of mergeSort.
  // It was initialized during the calculation in operator.
  public int inputChannelIndex;

  public MergeSortKey(TsBlock tsBlock, int rowIndex) {
    super(tsBlock, rowIndex);
  }

  public MergeSortKey(TsBlock tsBlock, int rowIndex, int inputChannelIndex) {
    super(tsBlock, rowIndex);
    this.inputChannelIndex = inputChannelIndex;
  }

  public MergeSortKey(SortKey sortKey) {
    super(sortKey.tsBlock, sortKey.rowIndex);
    this.inputChannelIndex = -1;
  }
}
