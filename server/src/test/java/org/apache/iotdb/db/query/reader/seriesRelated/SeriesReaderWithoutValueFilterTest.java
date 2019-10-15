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

package org.apache.iotdb.db.query.reader.seriesRelated;

import java.io.IOException;
import org.apache.iotdb.db.query.reader.IBatchReader;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.utils.TimeValuePair;
import org.junit.Assert;
import org.junit.Test;

public class SeriesReaderWithoutValueFilterTest {

  private SeriesReaderWithoutValueFilter reader1;
  private SeriesReaderWithoutValueFilter reader2;

  private void init() {
    IBatchReader batchReader1 = new FakedIBatchPoint(100, 1000, 7, 11);
    IPointReader pointReader = new FakedIPointReader(20, 500, 11, 19);
    reader1 = new SeriesReaderWithoutValueFilter(batchReader1, pointReader);

    IBatchReader batchReader2 = new FakedIBatchPoint(100, 1000, 7, 11);
    reader2 = new SeriesReaderWithoutValueFilter(batchReader2, null);
  }

  @Test
  public void test() throws IOException {
    init();
    testWithoutNullReader();
    testWithNullPointReader();
  }

  private void testWithoutNullReader() throws IOException {
    int cnt = 0;
    while (reader1.hasNext()) {
      TimeValuePair timeValuePair = reader1.next();
      cnt++;
      if ((timeValuePair.getTimestamp() - 20) % 11 == 0
          && timeValuePair.getTimestamp() < 20 + 500 * 11) {
        Assert.assertEquals(timeValuePair.getTimestamp() % 19, timeValuePair.getValue().getLong());
        continue;
      }
      if ((timeValuePair.getTimestamp() - 100) % 7 == 0) {
        Assert.assertEquals(timeValuePair.getTimestamp() % 11, timeValuePair.getValue().getLong());
      }
    }
    Assert.assertEquals(1430, cnt);
  }

  private void testWithNullPointReader() throws IOException {
    int cnt = 0;
    while (reader2.hasNext()) {
      TimeValuePair timeValuePair = reader2.next();
      Assert.assertEquals(timeValuePair.getTimestamp() % 11, timeValuePair.getValue().getLong());
      cnt++;
    }
    Assert.assertEquals(1000, cnt);
  }
}