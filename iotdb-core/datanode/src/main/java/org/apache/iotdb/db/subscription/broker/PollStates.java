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

package org.apache.iotdb.db.subscription.broker;

import org.apache.iotdb.metrics.core.utils.IoTDBMovingAverage;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Meter;

public class PollStates {

  private volatile long lastPolledTimestamp;
  private final Meter polledMeter;

  public PollStates() {
    this.lastPolledTimestamp = -1;
    this.polledMeter = new Meter(new IoTDBMovingAverage(), Clock.defaultClock());
  }

  public void mark() {
    lastPolledTimestamp = System.currentTimeMillis();
    polledMeter.mark();
  }

  public boolean shouldPrefetch() {
    return (System.currentTimeMillis() - lastPolledTimestamp) * polledMeter.getOneMinuteRate()
        > 1000;
  }
}
