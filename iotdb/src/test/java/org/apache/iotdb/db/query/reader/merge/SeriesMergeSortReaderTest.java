/**
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
package org.apache.iotdb.db.query.reader.merge;

import java.io.IOException;
import org.apache.iotdb.db.query.reader.IReader;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.apache.iotdb.db.utils.TsPrimitiveType;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.junit.Assert;
import org.junit.Test;

public class SeriesMergeSortReaderTest {

  @Test
  public void test2S() throws IOException {

    // 2 series
    test(new long[]{1, 2, 3, 4, 5, 6}, new long[]{2, 2, 2, 1, 2, 2}, new long[]{1, 2, 3, 4, 5},
        new long[]{1, 2, 3, 5, 6});
    test(new long[]{1, 2, 3, 4, 5}, new long[]{1, 1, 1, 1, 1}, new long[]{1, 2, 3, 4, 5},
        new long[]{});
    test(new long[]{1, 2, 3, 4, 5}, new long[]{2, 2, 2, 2, 2}, new long[]{},
        new long[]{1, 2, 3, 4, 5});
    test(new long[]{1, 2, 3, 4, 5, 6, 7, 8}, new long[]{1, 1, 1, 1, 1, 2, 2, 2},
        new long[]{1, 2, 3, 4, 5},
        new long[]{6, 7, 8});

    // 3 series
    test(new long[]{1, 2, 3, 4, 5, 6, 7}, new long[]{3, 3, 3, 1, 3, 2, 3},
        new long[]{1, 2, 3, 4, 5},
        new long[]{1, 2, 3, 5, 6}, new long[]{1, 2, 3, 5, 7});
    test(new long[]{1, 2, 3, 4, 5, 6}, new long[]{1, 1, 2, 3, 2, 3}, new long[]{1, 2},
        new long[]{3, 5},
        new long[]{4, 6});
  }

  private void test(long[] retTimestamp, long[] retValue, long[]... sources) throws IOException {
    PriorityMergeReader seriesMergeSortReader = new PriorityMergeReader();
    for (int i = 0; i < sources.length; i++) {
      seriesMergeSortReader.addReaderWithPriority(new FakedSeriesReader(sources[i], i + 1), i + 1);
    }

    int i = 0;
    while (seriesMergeSortReader.hasNext()) {
      TimeValuePair timeValuePair = seriesMergeSortReader.next();
      Assert.assertEquals(retTimestamp[i], timeValuePair.getTimestamp());
      Assert.assertEquals(retValue[i], timeValuePair.getValue().getValue());
      i++;
    }
  }

  public static class FakedSeriesReader implements IReader {

    private long[] timestamps;
    private int index;
    private long value;

    FakedSeriesReader(long[] timestamps, long value) {
      this.timestamps = timestamps;
      index = 0;
      this.value = value;
    }

    @Override
    public boolean hasNext() {
      return index < timestamps.length;
    }

    @Override
    public TimeValuePair next() {
      return new TimeValuePair(timestamps[index++], new TsPrimitiveType.TsLong(value));
    }

    @Override
    public void skipCurrentTimeValuePair() {
      next();
    }

    @Override
    public void close() {
    }

    @Override
    public boolean hasNextBatch() {
      return false;
    }

    @Override
    public BatchData nextBatch() {
      return null;
    }

    @Override
    public BatchData currentBatch() {
      return null;
    }
  }
}
