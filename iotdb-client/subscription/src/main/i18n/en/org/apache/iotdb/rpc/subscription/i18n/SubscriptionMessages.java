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

package org.apache.iotdb.rpc.subscription.i18n;

public final class SubscriptionMessages {

  // --- TopicConstant / ConsumerConstant ---
  public static final String UTILITY_CLASS = "Utility class";

  // --- SubscriptionPollRequest ---
  public static final String UNEXPECTED_REQUEST_TYPE =
      "unexpected request type: {}, payload will be null";

  // --- SubscriptionPollResponse ---
  public static final String UNEXPECTED_RESPONSE_TYPE =
      "unexpected response type: {}, payload will be null";

  // --- IdentifierUtils ---
  public static final String NULL_IDENTIFIER_NOT_SUPPORTED = "null identifier is not supported";
  public static final String EMPTY_IDENTIFIER_NOT_SUPPORTED = "empty identifier is not supported";

  // --- PollTimer ---
  public static final String INVALID_NEGATIVE_TIMEOUT = "Invalid negative timeout ";

  // --- AbstractSubscriptionPushConsumer ---
  public static final String PUSH_CONSUMER_CANCEL_AUTO_POLL =
      "SubscriptionPushConsumer {} cancel auto poll worker";
  public static final String PUSH_CONSUMER_SUBMIT_AUTO_POLL =
      "SubscriptionPushConsumer {} submit auto poll worker";
  public static final String PUSH_CONSUMER_POLL_EMPTY_MESSAGE =
      "SubscriptionPushConsumer {} poll empty message from topics {} after {} millisecond(s), "
          + "consecutive empty polls: {}";
  public static final String CONSUMER_LISTENER_FAILURE =
      "Consumer listener result failure when consuming message: {}";
  public static final String AUTO_POLL_UNEXPECTED = "something unexpected happened when auto poll messages...";

  // --- SubscriptionExecutorServiceManager ---
  public static final String EXECUTOR_LAUNCHING = "Launching {} with core pool size {}...";
  public static final String EXECUTOR_SHUTTING_DOWN = "Shutting down {}...";
  public static final String EXECUTOR_NOT_LAUNCHED_SUBMIT =
      "{} has not been launched, ignore submit task";
  public static final String EXECUTOR_NOT_LAUNCHED_INVOKE =
      "{} has not been launched, ignore invoke all tasks";
  public static final String EXECUTOR_NOT_LAUNCHED_ZERO =
      "{} has not been launched, return zero";
  public static final String EXECUTOR_NOT_LAUNCHED_SCHEDULE =
      "{} has not been launched, ignore scheduleWithFixedDelay for task";

  // --- AbstractSubscriptionProviders ---
  public static final String PROVIDER_CLOSE_FAILED =
      "Failed to close subscription provider {} because of {}";
  public static final String ADD_NEW_PROVIDER = "add new subscription provider {}";
  public static final String CLOSE_STALE_PROVIDER = "close and remove stale subscription provider {}";
  public static final String OPEN_PROVIDERS_FAILED =
      "Failed to open providers for consumer {} because of {}";
  public static final String FETCH_ENDPOINTS_FAILED =
      "Failed to fetch all endpoints for consumer {} because of {}";

  // --- AbstractSubscriptionPullConsumer ---
  public static final String PULL_CONSUMER_CANCEL_AUTO_COMMIT =
      "SubscriptionPullConsumer {} cancel auto commit worker";
  public static final String PULL_CONSUMER_SUBMIT_AUTO_COMMIT =
      "SubscriptionPullConsumer {} submit auto commit worker";
  public static final String PULL_CONSUMER_POLL_EMPTY_MESSAGE =
      "SubscriptionPullConsumer {} poll empty message from topics {} after {} millisecond(s), "
          + "consecutive empty polls: {}";
  public static final String AUTO_COMMIT_UNEXPECTED =
      "something unexpected happened when auto commit messages...";
  public static final String COMMIT_DURING_CLOSE_UNEXPECTED =
      "something unexpected happened when commit messages during close";

  // --- AbstractSubscriptionConsumer ---
  public static final String UNEXPECTED_RESPONSE_TYPE_WARN = "unexpected response type: {}";
  public static final String CONSUMER_CANCEL_HEARTBEAT_WORKER =
      "SubscriptionConsumer {} cancel heartbeat worker";
  public static final String CONSUMER_SUBMIT_HEARTBEAT_WORKER =
      "SubscriptionConsumer {} submit heartbeat worker";
  public static final String CONSUMER_CANCEL_ENDPOINTS_SYNCER =
      "SubscriptionConsumer {} cancel endpoints syncer";
  public static final String CONSUMER_SUBMIT_ENDPOINTS_SYNCER =
      "SubscriptionConsumer {} submit endpoints syncer";

  // --- TopicMeta (owner fencing) ---
  public static final String OWNER_ID_SHOULD_NOT_BE_EMPTY =
      "Subscription topic owner id should not be empty";
  public static final String OWNER_EPOCH_SHOULD_NOT_BE_NEGATIVE =
      "Subscription topic owner epoch should not be negative";
  public static final String OWNER_EPOCH_SHOULD_INCREASE_MONOTONICALLY =
      "Subscription topic owner epoch should increase monotonically, current epoch is %s, incoming"
          + " epoch is %s";
  public static final String OWNER_LEASE_DURATION_SHOULD_BE_POSITIVE =
      "Subscription topic owner lease duration should be positive, topic: %s,"
          + " owner-lease-duration-ms: %s";
  public static final String OWNER_EPOCH_SHOULD_NEVER_BE_REUSED =
      "Subscription topic owner epoch should never be reused, topic: %s, max granted owner-epoch:"
          + " %s, incoming owner-id: %s, incoming owner-epoch: %s";
  public static final String OWNER_SHOULD_NOT_BE_CLEARED_BY_STALE_META =
      "Subscription topic owner should not be cleared by stale topic meta, topic: %s, current"
          + " owner-id: %s, current owner-epoch: %s";
  public static final String OWNER_EPOCH_SHOULD_NOT_ROLL_BACK =
      "Subscription topic owner epoch should not roll back, topic: %s, current owner-id: %s, current"
          + " owner-epoch: %s, incoming owner-id: %s, incoming owner-epoch: %s";
  public static final String OWNER_EPOCH_SHOULD_BE_SET_WHEN_OWNER_ID_SET =
      "Subscription topic owner epoch should be set when %s is set";
  public static final String OWNER_LEASE_DURATION_SHOULD_BE_POSITIVE_WHEN_SET =
      "Subscription topic owner lease duration should be positive when %s is set";

  private SubscriptionMessages() {}
}
