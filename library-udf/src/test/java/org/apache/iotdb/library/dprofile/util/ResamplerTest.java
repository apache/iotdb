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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.library.dprofile.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ResamplerTest {

  @Test
  public void flushDoesNotTreatDefaultEndTimeAsAnEndpoint() {
    Resampler resampler = new Resampler(10, "last", "ffill");
    resampler.insert(-40, 1.0);
    resampler.insert(-30, 2.0);
    resampler.insert(-20, 3.0);

    resampler.flush();

    Assert.assertEquals(List.of(-40L, -30L, -20L), drainTimes(resampler));
  }

  @Test
  public void insertNearLongMaxDoesNotOverflowWindowBoundary() {
    Resampler resampler = new Resampler(10, "mean", "linear");
    long firstTime = Long.MAX_VALUE - 25;
    resampler.insert(firstTime, 0.0);
    resampler.insert(firstTime + 10, 10.0);
    resampler.insert(firstTime + 20, 20.0);
    resampler.insert(Long.MAX_VALUE, 30.0);

    resampler.flush();

    Assert.assertEquals(List.of(firstTime, firstTime + 10, firstTime + 20), drainTimes(resampler));
  }

  private List<Long> drainTimes(Resampler resampler) {
    List<Long> times = new ArrayList<>();
    while (resampler.hasNext()) {
      times.add(resampler.getOutTime());
      resampler.next();
    }
    return times;
  }
}
