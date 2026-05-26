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
      "Unexpected request type: {}, payload will be null";

  // --- SubscriptionPollResponse ---
  public static final String UNEXPECTED_RESPONSE_TYPE =
      "Unexpected response type: {}, payload will be null";

  // --- IdentifierUtils ---
  public static final String NULL_IDENTIFIER_NOT_SUPPORTED = "null identifier is not supported";
  public static final String EMPTY_IDENTIFIER_NOT_SUPPORTED = "empty identifier is not supported";

  // --- PollTimer ---
  public static final String INVALID_NEGATIVE_TIMEOUT = "Invalid negative timeout ";

  // --- AbstractSubscriptionPushConsumer ---
  public static final String PUSH_CONSUMER_CANCEL_AUTO_POLL =
      "SubscriptionPushConsumer {} is canceling the auto-poll worker";
  public static final String PUSH_CONSUMER_SUBMIT_AUTO_POLL =
      "SubscriptionPushConsumer {} is submitting the auto-poll worker";
  public static final String CONSUMER_LISTENER_FAILURE =
      "Consumer listener returned failure when consuming message: {}";
  public static final String AUTO_POLL_UNEXPECTED =
      "Unexpected exception while auto-polling messages.";

  // --- SubscriptionExecutorServiceManager ---
  public static final String EXECUTOR_LAUNCHING = "Launching {} with core pool size {}...";
  public static final String EXECUTOR_SHUTTING_DOWN = "Shutting down {}...";
  public static final String EXECUTOR_NOT_LAUNCHED_SUBMIT =
      "{} has not been launched, ignoring submitted task";
  public static final String EXECUTOR_NOT_LAUNCHED_INVOKE =
      "{} has not been launched, ignoring invokeAll tasks";
  public static final String EXECUTOR_NOT_LAUNCHED_ZERO =
      "{} has not been launched, returning zero";
  public static final String EXECUTOR_NOT_LAUNCHED_SCHEDULE =
      "{} has not been launched, ignoring scheduleWithFixedDelay task";

  // --- AbstractSubscriptionProviders ---
  public static final String PROVIDER_CLOSE_FAILED =
      "Failed to close subscription provider {} because {}";
  public static final String ADD_NEW_PROVIDER = "Adding new subscription provider {}";
  public static final String CLOSE_STALE_PROVIDER = "Closing and removing stale subscription provider {}";
  public static final String OPEN_PROVIDERS_FAILED =
      "Failed to open providers for consumer {} because {}";
  public static final String FETCH_ENDPOINTS_FAILED =
      "Failed to fetch all endpoints for consumer {} because {}";

  // --- AbstractSubscriptionPullConsumer ---
  public static final String PULL_CONSUMER_CANCEL_AUTO_COMMIT =
      "SubscriptionPullConsumer {} is canceling the auto-commit worker";
  public static final String PULL_CONSUMER_SUBMIT_AUTO_COMMIT =
      "SubscriptionPullConsumer {} is submitting the auto-commit worker";
  public static final String AUTO_COMMIT_UNEXPECTED =
      "Unexpected exception while auto-committing messages.";
  public static final String COMMIT_DURING_CLOSE_UNEXPECTED =
      "Unexpected exception while committing messages during close.";

  // --- AbstractSubscriptionConsumer ---
  public static final String UNEXPECTED_RESPONSE_TYPE_WARN = "Unexpected response type: {}";
  public static final String CONSUMER_CANCEL_HEARTBEAT_WORKER =
      "SubscriptionConsumer {} is canceling the heartbeat worker";
  public static final String CONSUMER_SUBMIT_HEARTBEAT_WORKER =
      "SubscriptionConsumer {} is submitting the heartbeat worker";
  public static final String CONSUMER_CANCEL_ENDPOINTS_SYNCER =
      "SubscriptionConsumer {} is canceling the endpoints syncer";
  public static final String CONSUMER_SUBMIT_ENDPOINTS_SYNCER =
      "SubscriptionConsumer {} is submitting the endpoints syncer";

  private SubscriptionMessages() {}
}
