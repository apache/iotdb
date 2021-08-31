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

public class PriorityMergeReaderTest {

  @Test
  public void test2S() throws IOException {

    // 2 series
    test(
        new long[] {1, 2, 3, 4, 5, 6},
        new long[] {2, 2, 2, 1, 2, 2},
        new long[] {1, 2, 3, 4, 5},
        new long[] {1, 2, 3, 5, 6});
    test(
        new long[] {1, 2, 3, 4, 5},
        new long[] {1, 1, 1, 1, 1},
        new long[] {1, 2, 3, 4, 5},
        new long[] {});
    test(
        new long[] {1, 2, 3, 4, 5},
        new long[] {2, 2, 2, 2, 2},
        new long[] {},
        new long[] {1, 2, 3, 4, 5});
    test(
        new long[] {1, 2, 3, 4, 5, 6, 7, 8},
        new long[] {1, 1, 1, 1, 1, 2, 2, 2},
        new long[] {1, 2, 3, 4, 5},
        new long[] {6, 7, 8});

    // 3 series
    test(
        new long[] {1, 2, 3, 4, 5, 6, 7},
        new long[] {3, 3, 3, 1, 3, 2, 3},
        new long[] {1, 2, 3, 4, 5},
        new long[] {1, 2, 3, 5, 6},
        new long[] {1, 2, 3, 5, 7});
    test(
        new long[] {1, 2, 3, 4, 5, 6},
        new long[] {1, 1, 2, 3, 2, 3},
        new long[] {1, 2},
        new long[] {3, 5},
        new long[] {4, 6});
  }

  private void test(long[] retTimestamp, long[] retValue, long[]... sources) throws IOException {
    PriorityMergeReader priorityMergeReader = new PriorityMergeReader();
    for (int i = 0; i < sources.length; i++) {
      priorityMergeReader.addReader(new FakedSeriesReader(sources[i], i + 1), i + 1);
    }

    int i = 0;
    while (priorityMergeReader.hasNextTimeValuePair()) {
      TimeValuePair timeValuePair = priorityMergeReader.nextTimeValuePair();
      Assert.assertEquals(retTimestamp[i], timeValuePair.getTimestamp());
      Assert.assertEquals(retValue[i], timeValuePair.getValue().getValue());
      i++;
    }
  }
}
