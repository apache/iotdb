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

package org.apache.iotdb.metrics.impl;

import org.apache.iotdb.metrics.enums.OutPutFormat;
import org.apache.iotdb.metrics.type.HistogramSnapshot;
import org.apache.iotdb.metrics.type.Rate;
import org.apache.iotdb.metrics.type.Timer;

import java.util.concurrent.TimeUnit;

public class DoNothingTimer implements Timer {
  @Override
  public void update(long duration, TimeUnit unit) {
    // do nothing
  }

  @Override
  public HistogramSnapshot takeSnapshot() {
    return new DoNothingHistogramSnapshot();
  }

  @Override
  public Rate getImmutableRate() {
    return new DoNothingRate();
  }

  @Override
  public StringBuffer toString(OutPutFormat format) {
    return null;
  }
}
