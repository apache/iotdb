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

package org.apache.iotdb.db.utils.windowing.configuration;

import org.apache.iotdb.db.utils.windowing.exception.WindowingException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class SlidingTimeWindowConfiguration extends Configuration {

  private final long timeInterval;
  private final long slidingStep;

  public SlidingTimeWindowConfiguration(TSDataType dataType, long timeInterval, long slidingStep) {
    super(dataType);
    this.timeInterval = timeInterval;
    this.slidingStep = slidingStep;
  }

  public SlidingTimeWindowConfiguration(TSDataType dataType, long timeInterval) {
    super(dataType);
    this.timeInterval = timeInterval;
    this.slidingStep = timeInterval;
  }

  @Override
  public void check() throws WindowingException {
    if (timeInterval <= 0) {
      throw new WindowingException(
          String.format("Parameter timeInterval(%d) should be positive.", timeInterval));
    }
    if (slidingStep <= 0) {
      throw new WindowingException(
          String.format("Parameter slidingStep(%d) should be positive.", slidingStep));
    }
  }

  public long getTimeInterval() {
    return timeInterval;
  }

  public long getSlidingStep() {
    return slidingStep;
  }
}
