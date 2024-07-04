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

package org.apache.iotdb.commons.enums;

import org.apache.iotdb.commons.pipe.config.PipeConfig;

import com.codahale.metrics.Meter;

public enum PipeRemainingTimeRateAverageTime {
  ONE_MINUTE,
  FIVE_MINUTES,
  FIFTEEN_MINUTES,
  MEAN;

  public double getMeterRate(final Meter meter) {
    switch (this) {
      case ONE_MINUTE:
        return meter.getOneMinuteRate();
      case FIVE_MINUTES:
        return meter.getFiveMinuteRate();
      case FIFTEEN_MINUTES:
        return meter.getFifteenMinuteRate();
      case MEAN:
        return meter.getMeanRate();
      default:
        throw new UnsupportedOperationException(
            String.format(
                "The type %s is not supported in average time of pipe remaining time.",
                PipeConfig.getInstance().getPipeRemainingTimeCommitRateAverageTime()));
    }
  }
}
