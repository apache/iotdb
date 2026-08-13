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

public class AcquirePolicy {

  public static final long DEFAULT_MAX_WAIT_MS = 100L;
  public static final long DEFAULT_RETRY_INTERVAL_MS = 10L;

  private long maxWaitMs = DEFAULT_MAX_WAIT_MS;
  private long retryIntervalMs = DEFAULT_RETRY_INTERVAL_MS;

  public long getMaxWaitMs() {
    return maxWaitMs;
  }

  public void setMaxWaitMs(long maxWaitMs) {
    this.maxWaitMs = maxWaitMs;
  }

  public long getRetryIntervalMs() {
    return retryIntervalMs;
  }

  public void setRetryIntervalMs(long retryIntervalMs) {
    this.retryIntervalMs = retryIntervalMs;
  }

  public static AcquirePolicy defaults() {
    return new AcquirePolicy();
  }
}
