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

package org.apache.iotdb.session.subscription.util;

public class SubscriptionPollTimer {

  private long startMs;
  private long currentTimeMs;
  private long deadlineMs;
  private long timeoutMs;

  public SubscriptionPollTimer(long startMs, long timeoutMs) {
    this.update(startMs);
    this.reset(timeoutMs);
  }

  public boolean isExpired() {
    return this.currentTimeMs >= this.deadlineMs;
  }

  public boolean notExpired() {
    return !this.isExpired();
  }

  public void reset(long timeoutMs) {
    if (timeoutMs < 0L) {
      throw new IllegalArgumentException("Invalid negative timeout " + timeoutMs);
    } else {
      this.timeoutMs = timeoutMs;
      this.startMs = this.currentTimeMs;
      if (this.currentTimeMs > Long.MAX_VALUE - timeoutMs) {
        this.deadlineMs = Long.MAX_VALUE;
      } else {
        this.deadlineMs = this.currentTimeMs + timeoutMs;
      }
    }
  }

  public void update() {
    update(System.currentTimeMillis());
  }

  public void update(long currentTimeMs) {
    this.currentTimeMs = Math.max(currentTimeMs, this.currentTimeMs);
  }

  public long remainingMs() {
    return Math.max(0L, this.deadlineMs - this.currentTimeMs);
  }

  public long currentTimeMs() {
    return this.currentTimeMs;
  }

  public long elapsedMs() {
    return this.currentTimeMs - this.startMs;
  }

  public long timeoutMs() {
    return this.timeoutMs;
  }
}
