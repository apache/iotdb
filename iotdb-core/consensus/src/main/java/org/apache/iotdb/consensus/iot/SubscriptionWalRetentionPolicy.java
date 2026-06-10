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

package org.apache.iotdb.consensus.iot;

public class SubscriptionWalRetentionPolicy {

  public static final long UNBOUNDED = -1L;

  private final String topicName;
  private final long retentionBytes;
  private final long retentionMs;

  public SubscriptionWalRetentionPolicy(
      final String topicName, final long retentionBytes, final long retentionMs) {
    this.topicName = topicName;
    this.retentionBytes = retentionBytes;
    this.retentionMs = retentionMs;
  }

  public String getTopicName() {
    return topicName;
  }

  public long getRetentionBytes() {
    return retentionBytes;
  }

  public long getRetentionMs() {
    return retentionMs;
  }

  public boolean isBytesUnbounded() {
    return retentionBytes == UNBOUNDED;
  }

  public boolean isTimeUnbounded() {
    return retentionMs == UNBOUNDED;
  }
}
