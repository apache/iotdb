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

package org.apache.iotdb.db.query.reader.universal;

import org.apache.iotdb.tsfile.read.TimeValuePair;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class PriorityMergeReaderTest2 {

  @Test
  public void test() throws IOException {
    FakedSeriesReader reader1 = new FakedSeriesReader(100, 80, 5, 11);
    FakedSeriesReader reader2 = new FakedSeriesReader(150, 60, 6, 19);
    FakedSeriesReader reader3 = new FakedSeriesReader(180, 50, 7, 31);

    PriorityMergeReader priorityMergeReader = new PriorityMergeReader();
    priorityMergeReader.addReader(reader1, 3);
    priorityMergeReader.addReader(reader2, 2);
    priorityMergeReader.addReader(reader3, 1);

    int cnt = 0;
    while (priorityMergeReader.hasNextTimeValuePair()) {
      TimeValuePair timeValuePair = priorityMergeReader.nextTimeValuePair();
      long time = timeValuePair.getTimestamp();
      long value = (long) timeValuePair.getValue().getValue();

      // System.out.println(time + "," + value);
      if (time <= 500 && (time - 100) % 5 == 0) {
        Assert.assertEquals(time % 11, value);
      } else if (time <= 510 && (time - 150) % 6 == 0) {
        Assert.assertEquals(time % 19, value);
      } else {
        Assert.assertEquals(time % 31, value);
      }
      cnt++;
    }
    Assert.assertEquals(162, cnt);
  }
}
