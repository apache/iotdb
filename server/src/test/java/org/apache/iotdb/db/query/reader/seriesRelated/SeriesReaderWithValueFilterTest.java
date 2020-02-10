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
import org.apache.iotdb.db.query.reader.ManagedSeriesReader;
import org.apache.iotdb.tsfile.read.reader.IBatchReader;
import org.apache.iotdb.tsfile.read.filter.ValueFilter;
import org.junit.Test;

public class SeriesReaderWithValueFilterTest {

  private ManagedSeriesReader reader;

  private void init() throws IOException {
    // (100,0),(105,1),(110,0),(115,1),(120,0),...
    IBatchReader batchReader = new FakedIBatchPoint(100, 1000, 5, 2);
    // (100,0),(105,1),(110,2),(115,3),(120,0),...
    IBatchReader pointReader = new FakedIBatchPoint(100, 500, 5, 4);
    reader = new SeriesReaderWithValueFilter(batchReader, pointReader, ValueFilter.eq(0L));
  }

  @Test
  public void test() throws IOException {
    init();
    int cnt = 0;
    long startTime = 100; // 100-20
//    while (reader.hasNext()) {
//      TimeValuePair timeValuePair = reader.next();
//      if (cnt < 125) {
//        Assert.assertEquals(startTime, timeValuePair.getTimestamp());
//        startTime += 20;
//      } else {
//        Assert.assertEquals(startTime, timeValuePair.getTimestamp());
//        startTime += 10;
//      }
//      cnt++;
//    }
//    Assert.assertEquals(375, cnt);
  }

}
