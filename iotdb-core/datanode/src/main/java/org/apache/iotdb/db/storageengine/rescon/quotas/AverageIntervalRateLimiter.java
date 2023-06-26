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

package org.apache.iotdb.db.storageengine.rescon.quotas;

/**
 * This limiter will refill resources at every TimeUnit/resources interval. For example: For a
 * limiter configured with 10resources/second, then 1 resource will be refilled after every 100ms
 * (1sec/10resources). Copy from hbase.
 */
public class AverageIntervalRateLimiter extends RateLimiter {

  private long nextRefillTime = -1L;

  @Override
  public long refill(long limit) {
    final long now = System.currentTimeMillis();
    if (nextRefillTime == -1) {
      // Till now no resource has been consumed.
      nextRefillTime = System.currentTimeMillis();
      return limit;
    }

    long timeInterval = now - nextRefillTime;
    long delta = 0;
    long timeUnitInMillis = super.getTimeUnitInMillis();
    if (timeInterval >= timeUnitInMillis) {
      delta = limit;
    } else if (timeInterval > 0) {
      double r = ((double) timeInterval / (double) timeUnitInMillis) * limit;
      delta = (long) r;
    }

    if (delta > 0) {
      this.nextRefillTime = now;
    }

    return delta;
  }

  @Override
  public long getWaitInterval(long limit, long available, long amount) {
    if (nextRefillTime == -1) {
      return 0;
    }

    double r = ((double) (amount - available)) * super.getTimeUnitInMillis() / limit;
    return (long) r;
  }

  // This method is for strictly testing purpose only
  public void setNextRefillTime(long nextRefillTime) {
    this.nextRefillTime = nextRefillTime;
  }

  public long getNextRefillTime() {
    return this.nextRefillTime;
  }
}
