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

package org.apache.iotdb.db.mpp.aggregation;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public class ModeAccumulator implements Accumulator {
  private final Map<Integer, Long> countMap = new HashMap<>(); // ftl

  private final int MAP_SIZE_THRESHOLD = 10000;

  @Override
  public void addInput(Column[] column, BitMap bitMap, int lastIndex) {
    for (int i = 0; i <= lastIndex; i++) {
      if (bitMap != null && !bitMap.isMarked(i)) {
        continue;
      }
      if (!column[1].isNull(i)) {
        countMap.compute(column[1].getInt(i), (k, v) -> v == null ? 1 : v + 1); // ftl

        if (countMap.size() > MAP_SIZE_THRESHOLD) {
          throw new RuntimeException(
              String.format(
                  "size of countMap in ModeAccumulator has exceed the threshold %s",
                  MAP_SIZE_THRESHOLD));
        }
      }
    }
  }

  @Override
  public void addIntermediate(Column[] partialResult) {
    checkArgument(partialResult.length == 1, "partialResult of Mode should be 1");
    checkArgument(!partialResult[0].isNull(0), "partialResult of Mode should not be null");
    deserializeAndMergeCountMap(partialResult[0].getBinary(0));
  }

  @Override
  public void addStatistics(Statistics statistics) {
    throw new UnsupportedOperationException(getClass().getName());
  }

  @Override
  public void setFinal(Column finalResult) {
    if (finalResult.isNull(0)) {
      return;
    }

    // Step of ModeAccumulator is STATIC,
    // countMap only need to record one entry which key is finalResult
    countMap.put(finalResult.getInt(0), 1L); // ftl
  }

  @Override
  public void outputIntermediate(ColumnBuilder[] tsBlockBuilder) {
    tsBlockBuilder[0].writeBinary(serializeCountMap());
  }

  @Override
  public void outputFinal(ColumnBuilder tsBlockBuilder) {
    if (countMap.isEmpty()) {
      tsBlockBuilder.appendNull();
    } else {
      tsBlockBuilder.writeInt( // ftl
          Collections.max(countMap.entrySet(), Map.Entry.comparingByValue()).getKey());
    }
  }

  @Override
  public void reset() {
    countMap.clear();
  }

  @Override
  public boolean hasFinalResult() {
    return false;
  }

  @Override
  public TSDataType[] getIntermediateType() {
    return new TSDataType[] {TSDataType.TEXT};
  }

  @Override
  public TSDataType getFinalType() {
    return TSDataType.INT32; // ftl
  }

  private Binary serializeCountMap() {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    try {
      ReadWriteIOUtils.write(countMap.size(), stream);
      for (Map.Entry<Integer, Long> entry : countMap.entrySet()) { // ftl
        ReadWriteIOUtils.write(entry.getKey(), stream);
        ReadWriteIOUtils.write(entry.getValue(), stream);
      }
    } catch (IOException e) {
      // Totally memory operation. This case won't happen.
    }
    return new Binary(stream.toByteArray());
  }

  private void deserializeAndMergeCountMap(Binary partialResult) {
    InputStream stream = new ByteArrayInputStream(partialResult.getValues());
    try {
      for (int i = 0; i < ReadWriteIOUtils.readInt(stream); i++) { // ftl
        countMap.put(ReadWriteIOUtils.readInt(stream), ReadWriteIOUtils.readLong(stream)); // ftl

        if (countMap.size() > MAP_SIZE_THRESHOLD) {
          throw new RuntimeException(
              String.format(
                  "size of countMap in ModeAccumulator has exceed the threshold %s",
                  MAP_SIZE_THRESHOLD));
        }
      }
    } catch (IOException e) {
      // Totally memory operation. This case won't happen.
    }
  }
}
