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
package org.apache.iotdb.db.mpp.execution.operator.process.join.merge;

import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlock.TsBlockSingleColumnIterator;

import java.util.List;

public interface MergeSortToolKit {

  /** add TsBlock for specific child it usually called when last one was run out */
  void addTsBlock(TsBlock tsBlock, int index);

  /** update consumed result */
  void updateTsBlock(int index, TsBlock tsBlock);
  /**
   * get the index of TsBlock whose startValue<=targetValue when ordering is asc,and
   * startValue>=targetValue when ordering is desc. targetValue is the smallest endKey when ordering
   * is asc and the biggest endKey when ordering is desc.
   */
  List<Integer> getTargetTsBlockIndex();

  // +-----------------------+
  // |  keyValue comparator  |
  // +-----------------------+

  /** check if the keyValue in tsBlockIterator is less than current targetValue */
  boolean satisfyCurrentEndValue(TsBlockSingleColumnIterator tsBlockIterator);

  /** check if t is greater than s greater means the one with bigger rowIndex in result set */
  boolean greater(TsBlockSingleColumnIterator t, TsBlockSingleColumnIterator s);
}
