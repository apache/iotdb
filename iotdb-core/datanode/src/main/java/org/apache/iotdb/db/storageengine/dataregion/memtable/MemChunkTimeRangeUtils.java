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

package org.apache.iotdb.db.storageengine.dataregion.memtable;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

final class MemChunkTimeRangeUtils {

  private MemChunkTimeRangeUtils() {
    // Utility class.
  }

  static List<long[]> splitFakePageTimeRanges(long chunkStartTime, long chunkEndTime, int pageNum) {
    BigInteger timeRange =
        BigInteger.valueOf(chunkEndTime)
            .subtract(BigInteger.valueOf(chunkStartTime))
            .add(BigInteger.ONE);
    int effectivePageNum =
        timeRange.compareTo(BigInteger.valueOf(pageNum)) < 0 ? timeRange.intValue() : pageNum;
    BigInteger pageTimeInterval = timeRange.divide(BigInteger.valueOf(effectivePageNum));
    BigInteger chunkStartTimeAsBigInteger = BigInteger.valueOf(chunkStartTime);

    List<long[]> pageTimeRanges = new ArrayList<>(effectivePageNum);
    for (int i = 0; i < effectivePageNum; i++) {
      BigInteger pageStartTime =
          chunkStartTimeAsBigInteger.add(pageTimeInterval.multiply(BigInteger.valueOf(i)));
      BigInteger pageEndTime =
          i == effectivePageNum - 1
              ? BigInteger.valueOf(chunkEndTime)
              : chunkStartTimeAsBigInteger
                  .add(pageTimeInterval.multiply(BigInteger.valueOf(((long) i) + 1)))
                  .subtract(BigInteger.ONE);
      pageTimeRanges.add(new long[] {pageStartTime.longValue(), pageEndTime.longValue()});
    }
    return pageTimeRanges;
  }
}
