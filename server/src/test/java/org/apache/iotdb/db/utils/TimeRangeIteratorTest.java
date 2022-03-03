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

package org.apache.iotdb.db.utils;

import org.apache.iotdb.tsfile.utils.Pair;

import org.junit.Assert;
import org.junit.Test;

public class TimeRangeIteratorTest {

  @Test
  public void testNotSplitTimeRange() {
    String[] res = {
      "<0,4>", "<3,7>", "<6,10>", "<9,13>", "<12,16>", "<15,19>", "<18,22>", "<21,25>", "<24,28>",
      "<27,31>", "<30,32>"
    };

    long startTime = 0, endTime = 32, interval = 4, slidingStep = 3;

    TimeRangeIterator timeRangeIterator =
        new TimeRangeIterator(startTime, endTime, interval, slidingStep, true, false, false, false);

    checkRes(timeRangeIterator, res);

    TimeRangeIterator descTimeRangeIterator =
        new TimeRangeIterator(
            startTime, endTime, interval, slidingStep, false, false, false, false);

    checkRes(descTimeRangeIterator, res);
  }

  @Test
  public void testSplitTimeRange() {
    String[] res4_1 = {
      "<0,1>", "<1,2>", "<2,3>", "<3,4>", "<4,5>", "<5,6>", "<6,7>", "<7,8>", "<8,9>", "<9,10>",
      "<10,11>", "<11,12>", "<12,13>", "<13,14>", "<14,15>", "<15,16>", "<16,17>", "<17,18>",
      "<18,19>", "<19,20>", "<20,21>", "<21,22>", "<22,23>", "<23,24>", "<24,25>", "<25,26>",
      "<26,27>", "<27,28>", "<28,29>", "<29,30>", "<30,31>", "<31,32>"
    };
    String[] res4_2 = {
      "<0,2>", "<2,4>", "<4,6>", "<6,8>", "<8,10>", "<10,12>", "<12,14>", "<14,16>", "<16,18>",
      "<18,20>", "<20,22>", "<22,24>", "<24,26>", "<26,28>", "<28,30>", "<30,32>"
    };
    String[] res4_3 = {
      "<0,1>", "<1,3>", "<3,4>", "<4,6>", "<6,7>", "<7,9>", "<9,10>", "<10,12>", "<12,13>",
      "<13,15>", "<15,16>", "<16,18>", "<18,19>", "<19,21>", "<21,22>", "<22,24>", "<24,25>",
      "<25,27>", "<27,28>", "<28,30>", "<30,31>", "<31,32>"
    };
    String[] res4_4 = {
      "<0,4>", "<4,8>", "<8,12>", "<12,16>", "<16,20>", "<20,24>", "<24,28>", "<28,32>"
    };
    String[] res4_5 = {"<0,4>", "<5,9>", "<10,14>", "<15,19>", "<20,24>", "<25,29>", "<30,32>"};
    String[] res4_6 = {"<0,4>", "<6,10>", "<12,16>", "<18,22>", "<24,28>", "<30,32>"};

    checkRes(new TimeRangeIterator(0, 32, 4, 1, true, false, false, true), res4_1);
    checkRes(new TimeRangeIterator(0, 32, 4, 2, true, false, false, true), res4_2);
    checkRes(new TimeRangeIterator(0, 32, 4, 3, true, false, false, true), res4_3);
    checkRes(new TimeRangeIterator(0, 32, 4, 4, true, false, false, true), res4_4);
    checkRes(new TimeRangeIterator(0, 32, 4, 5, true, false, false, true), res4_5);
    checkRes(new TimeRangeIterator(0, 32, 4, 6, true, false, false, true), res4_6);
    checkRes(new TimeRangeIterator(0, 32, 4, 1, false, false, false, true), res4_1);
    checkRes(new TimeRangeIterator(0, 32, 4, 2, false, false, false, true), res4_2);
    checkRes(new TimeRangeIterator(0, 32, 4, 3, false, false, false, true), res4_3);
    checkRes(new TimeRangeIterator(0, 32, 4, 4, false, false, false, true), res4_4);
    checkRes(new TimeRangeIterator(0, 32, 4, 5, false, false, false, true), res4_5);
    checkRes(new TimeRangeIterator(0, 32, 4, 6, false, false, false, true), res4_6);
  }

  private void checkRes(TimeRangeIterator timeRangeIterator, String[] res) {
    boolean isAscending = timeRangeIterator.isAscending();
    long curStartTime;
    int cnt = isAscending ? 0 : res.length - 1;

    // test first time range
    Pair<Long, Long> firstTimeRange = timeRangeIterator.getFirstTimeRange();
    Assert.assertEquals(res[cnt], firstTimeRange.toString());
    cnt += isAscending ? 1 : -1;
    curStartTime = firstTimeRange.left;

    // test next time ranges
    Pair<Long, Long> curTimeRange = timeRangeIterator.getNextTimeRange(curStartTime, true);
    while (curTimeRange != null) {
      Assert.assertEquals(res[cnt], curTimeRange.toString());
      cnt += isAscending ? 1 : -1;
      curStartTime = curTimeRange.left;
      curTimeRange = timeRangeIterator.getNextTimeRange(curStartTime, true);
    }
  }
}
