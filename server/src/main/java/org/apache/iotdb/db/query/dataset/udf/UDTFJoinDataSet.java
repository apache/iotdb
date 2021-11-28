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

package org.apache.iotdb.db.query.dataset.udf;

import org.apache.iotdb.db.query.dataset.DirectAlignByTimeDataSet;
import org.apache.iotdb.db.tools.watermark.WatermarkEncoder;
import org.apache.iotdb.db.utils.datastructure.TimeSelector;
import org.apache.iotdb.service.rpc.thrift.TSQueryDataSet;
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import java.io.IOException;

// TODO: performances joining in pool, packing row records while calculating
public class UDTFJoinDataSet extends QueryDataSet implements DirectAlignByTimeDataSet {

  private final UDTFDataSet[] fragmentDataSets;

  /**
   * Each output column of the UDTFJoinDataSet corresponds to a two-tuple ({@code int[]}) instance
   * in queryDataSetOutputIndexToFragmentDataSetOutputIndex, through the two-tuple instance, the
   * dataset can get the corresponding output column in {@link UDTFJoinDataSet#fragmentDataSets}.
   *
   * <p>About the two-tuple:
   *
   * <p>The first element is the index of the fragmentDataSet which outputs the column.
   *
   * <p>The second element is the index of the actual output column of the given fragmentDataSet.
   */
  private final int[][] resultColumnOutputIndexToFragmentDataSetOutputIndex;

  private final int resultColumnsLength;
  private final RowRecord[] rowRecordsCache;
  private TimeSelector timeHeap;

  public UDTFJoinDataSet(
      UDTFDataSet[] fragmentDataSets, int[][] resultColumnOutputIndexToFragmentDataSetOutputIndex)
      throws IOException {
    this.fragmentDataSets = fragmentDataSets;
    this.resultColumnOutputIndexToFragmentDataSetOutputIndex =
        resultColumnOutputIndexToFragmentDataSetOutputIndex;
    resultColumnsLength = resultColumnOutputIndexToFragmentDataSetOutputIndex.length;
    rowRecordsCache = new RowRecord[resultColumnsLength];

    initTimeHeap();
  }

  private void initTimeHeap() throws IOException {
    timeHeap = new TimeSelector(resultColumnsLength << 1, true);

    for (int i = 0; i < resultColumnsLength; ++i) {
      UDTFDataSet fragmentDataSet = fragmentDataSets[i];
      if (fragmentDataSet.hasNextWithoutConstraint()) {
        rowRecordsCache[i] = fragmentDataSet.nextWithoutConstraint();
        timeHeap.add(rowRecordsCache[i].getTimestamp());
      }
    }
  }

  @Override
  public boolean hasNextWithoutConstraint() throws IOException {
    return !timeHeap.isEmpty();
  }

  @Override
  public RowRecord nextWithoutConstraint() throws IOException {
    long minTime = timeHeap.pollFirst();
    RowRecord rowRecord = new RowRecord(minTime, resultColumnsLength);

    for (int i = 0; i < resultColumnsLength; ++i) {
      int[] indexes = resultColumnOutputIndexToFragmentDataSetOutputIndex[i];
      int fragmentDataSetIndex = indexes[0];
      int outputColumnIndexInFragmentDataSet = indexes[1];

      if (rowRecordsCache[fragmentDataSetIndex] == null) {
        rowRecord.addField(null);
        continue;
      }

      RowRecord fragmentRowRecord = rowRecordsCache[fragmentDataSetIndex];
      if (fragmentRowRecord.getTimestamp() != minTime) {
        rowRecord.addField(null);
        continue;
      }

      rowRecord.addField(fragmentRowRecord.getFields().get(outputColumnIndexInFragmentDataSet));
      rowRecordsCache[fragmentDataSetIndex] = null;

      if (fragmentDataSets[fragmentDataSetIndex].hasNextWithoutConstraint()) {
        fragmentRowRecord = fragmentDataSets[fragmentDataSetIndex].nextWithoutConstraint();
        rowRecordsCache[fragmentDataSetIndex] = fragmentRowRecord;
        timeHeap.add(fragmentRowRecord.getTimestamp());
      }
    }

    return rowRecord;
  }

  @Override
  public TSQueryDataSet fillBuffer(int fetchSize, WatermarkEncoder encoder) {
    throw new NotImplementedException();
  }
}
