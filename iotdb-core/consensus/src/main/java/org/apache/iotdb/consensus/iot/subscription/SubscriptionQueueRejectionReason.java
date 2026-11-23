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

package org.apache.iotdb.consensus.iot.subscription;

/** Stable error codes used when a consensus subscription queue rejects realtime input. */
public enum SubscriptionQueueRejectionReason {
  NONE("NONE"),
  QUEUE_CAPACITY("SUBSCRIPTION_QUEUE_CAPACITY"),
  SUBSCRIPTION_MEMORY_QUOTA("SUBSCRIPTION_MEMORY_QUOTA"),
  SUBSCRIPTION_MEMORY_LIMIT("SUBSCRIPTION_MEMORY_LIMIT"),
  SUBSCRIPTION_OVERSIZED_ENTRY("SUBSCRIPTION_OVERSIZED_ENTRY"),
  WRITER_BACKLOG("SUBSCRIPTION_WRITER_BACKLOG"),
  SEEK_IN_PROGRESS("SUBSCRIPTION_SEEK_IN_PROGRESS"),
  CONSENSUS_REQUEST_MEMORY_LIMIT("CONSENSUS_REQUEST_MEMORY_LIMIT"),
  INACTIVE_OR_CLOSED("SUBSCRIPTION_QUEUE_INACTIVE_OR_CLOSED");

  private final String code;

  SubscriptionQueueRejectionReason(final String code) {
    this.code = code;
  }

  public String getCode() {
    return code;
  }
}
