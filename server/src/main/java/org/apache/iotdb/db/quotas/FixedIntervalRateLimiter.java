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

package org.apache.iotdb.db.quotas;

/**
 * With this limiter resources will be refilled only after a fixed interval of time. Copy from
 * hbase.
 */
public class FixedIntervalRateLimiter extends RateLimiter {

  private long nextRefillTime = -1L;

  @Override
  public long refill(long limit) {
    final long now = System.currentTimeMillis();
    if (now < nextRefillTime) {
      return 0;
    }
    nextRefillTime = now + super.getTimeUnitInMillis();
    return limit;
  }

  @Override
  public long getWaitInterval(long limit, long available, long amount) {
    if (nextRefillTime == -1) {
      return 0;
    }
    final long now = System.currentTimeMillis();
    final long refillTime = nextRefillTime;
    return refillTime - now;
  }

  public long getNextRefillTime() {
    return nextRefillTime;
  }

  public void setNextRefillTime(long nextRefillTime) {
    this.nextRefillTime = nextRefillTime;
  }
}
