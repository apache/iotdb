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

package org.apache.iotdb.db.subscription.receiver;

import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeRequestVersion;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeReq;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeResp;

public interface SubscriptionReceiver {

  TPipeSubscribeResp handle(TPipeSubscribeReq req);

  void setAuthenticatedUsername(final String username);

  PipeSubscribeRequestVersion getVersion();

  void handleExit();

  void handleTimeout();

  /**
   * Returns the identity of the consumer currently associated with this receiver, or {@code null}
   * if the receiver has not completed a handshake (or has already been invalidated).
   */
  String getConsumerId();

  /**
   * Returns the consumer group currently associated with this receiver, or {@code null} if the
   * receiver has not completed a handshake (or has already been invalidated).
   */
  String getConsumerGroupId();

  /**
   * Invalidates this receiver so that requests from an obsolete connection cannot affect a new
   * owner.
   */
  void invalidateConsumer();

  /** Returns whether this receiver still owns an active consumer timeout state. */
  boolean hasActiveConsumer();

  long remainingMs();
}
