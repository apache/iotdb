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
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String EXCEPTION_UNEXPECTED_FIELDS_ARG_WAS_OBTAINED_DURING_SHOW_TOPIC_30B5D702 = "Unexpected fields %s was obtained during SHOW TOPIC...";
  public static final String EXCEPTION_UNEXPECTED_FIELDS_ARG_WAS_OBTAINED_DURING_SHOW_SUBSCRIPTION_71F9C549 = "Unexpected fields %s was obtained during SHOW SUBSCRIPTION...";
  public static final String EXCEPTION_SUBSCRIPTION_SESSION_MUST_CONFIGURED_ENDPOINT_2CAD53A9 = "Subscription session must be configured with an endpoint.";
  public static final String EXCEPTION_ARG_ILLEGAL_IDENTIFIER_NOT_ENCLOSED_BACKTICKS_CAN_ONLY_CONSIST_DIGITS_55D6A31F =
      "%s is illegal, identifier not enclosed with backticks can only consist of digits, characters"
      + " and underscore.";
  public static final String EXCEPTION_WATERMARK_TIMESTAMP_ONLY_AVAILABLE_WATERMARK_MESSAGES_ACTUAL_MESSAGE_TYPE_F8E32C57 = "Watermark timestamp is only available for watermark messages, actual message type: ";
  public static final String EXCEPTION_ARG_DO_NOT_SUPPORT_GETRESULTSETS_7789852D = "%s do not support getResultSets().";
  public static final String EXCEPTION_ARG_DO_NOT_SUPPORT_GETRECORDTABLETITERATOR_46B4A489 = "%s do not support getRecordTabletIterator().";
  public static final String EXCEPTION_ARG_DO_NOT_SUPPORT_GETTSFILE_40D23462 = "%s do not support getTsFile().";
  public static final String EXCEPTION_USER_DATA_HAS_BEEN_REMOVED_ARG_7093644B = "User data has been removed from %s.";
  public static final String EXCEPTION_ARG_DOES_NOT_SUPPORT_OPENTREEREADER_TABLE_MODEL_TSFILE_68621ECF = "%s does not support openTreeReader() for table model tsfile.";
  public static final String EXCEPTION_ARG_DOES_NOT_SUPPORT_OPENTABLEREADER_TREE_MODEL_TSFILE_CEC27860 = "%s does not support openTableReader() for tree model tsfile.";
  public static final String EXCEPTION_UNKNOWN_COLUMN_CATEGORY_4F49F64B = "Unknown column category: ";
  public static final String EXCEPTION_DATA_TYPE_ARG_NOT_SUPPORTED_31213160 = "Data type %s is not supported.";
  public static final String LOG_SUBSCRIPTIONPUSHCONSUMER_ARG_POLL_EMPTY_MESSAGE_TOPICS_ARG_AFTER_ARG_MILLISECOND_741801C2 = "SubscriptionPushConsumer {} poll empty message from topics {} after {} millisecond(s)";
  public static final String LOG_CONSUMER_LISTENER_RAISED_EXCEPTION_CONSUMING_MESSAGE_ARG_867EE46D = "Consumer listener raised an exception while consuming message: {}";
  public static final String LOG_ARG_FAILED_CREATE_CONNECTION_ARG_BECAUSE_ARG_E536E22A = "{} failed to create connection with {} because of {}";
  public static final String LOG_ARG_FAILED_FETCH_ALL_ENDPOINTS_ARG_BECAUSE_ARG_2C9E11D4 = "{} failed to fetch all endpoints from {} because of {}";
  public static final String LOG_TERMINATION_OCCURRED_SUBSCRIPTIONCONSUMER_ARG_POLLING_TOPICS_UNSUBSCRIBE_TOPIC_ARG_AUTOMATICALLY_E9B695FA =
      "Termination occurred when SubscriptionConsumer {} polling topics, unsubscribe topic {}"
      + " automatically";
  public static final String LOG_ARG_FAILED_SENDING_HEARTBEAT_SUBSCRIPTION_PROVIDER_ARG_BECAUSE_ARG_SET_0B38FB1F =
      "{} failed to sending heartbeat to subscription provider {} because of {}, set subscription"
      + " provider unavailable";
  public static final String LOG_EXCEPTION_OCCURRED_ARG_CLOSING_REMOVING_SUBSCRIPTION_PROVIDER_ARG_BECAUSE_ARG_2EC38739 = "Exception occurred when {} closing and removing subscription provider {} because of {}";
  public static final String EXCEPTION_CLUSTER_HAS_NO_AVAILABLE_SUBSCRIPTION_PROVIDERS_CONNECT_INITIAL_ENDPOINTS_ARG_5DB83198 = "Cluster has no available subscription providers to connect with initial endpoints %s";
  public static final String LOG_ARG_HAS_BEEN_LAUNCHED_SET_CORE_POOL_SIZE_ARG_WILL_0FDECBE3 = "{} has been launched, set core pool size to {} will be ignored";
  public static final String LOG_INTERRUPT_WORKER_WHICH_MAY_CAUSE_SOME_TASK_INCONSISTENT_PLEASE_CHECK_04926D9F = "Interrupt the worker, which may cause some task inconsistent. Please check the biz logs.";
  public static final String LOG_THREAD_POOL_CAN_T_SHUTDOWN_EVEN_INTERRUPTING_WORKER_THREADS_WHICH_A49166F9 =
      "Thread pool can't be shutdown even with interrupting worker threads, which may cause some"
      + " task inconsistent. Please check the biz logs.";
  public static final String LOG_CURRENT_THREAD_INTERRUPTED_IT_TRYING_STOP_WORKER_THREADS_MAY_LEAVE_CF07ABA0 =
      "The current thread is interrupted when it is trying to stop the worker threads. This may"
      + " leave an inconsistent state. Please check the biz logs.";
  public static final String LOG_IOEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_SERIALIZE_HANDSHAKE_REQUEST_ARG_C1414290 = "IOException occurred when SubscriptionProvider {} serialize handshake request {}";
  public static final String LOG_TEXCEPTION_IOTDBCONNECTIONEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_HANDSHAKE_REQUEST_ARG_SET_SUBSCRIPTI =
      "TException/IoTDBConnectionException occurred when SubscriptionProvider {} handshake with"
      + " request {}, set SubscriptionProvider unavailable";
  public static final String LOG_TEXCEPTION_IOTDBCONNECTIONEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_CLOSE_SET_SUBSCRIPTIONPROVIDER_UNAVA =
      "TException/IoTDBConnectionException occurred when SubscriptionProvider {} close, set"
      + " SubscriptionProvider unavailable";
  public static final String LOG_IOEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_SERIALIZE_HEARTBEAT_REQUEST_ARG_634C8333 = "IOException occurred when SubscriptionProvider {} serialize heartbeat request {}";
  public static final String LOG_TEXCEPTION_IOTDBCONNECTIONEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_HEARTBEAT_SET_SUBSCRIPTIONPROVIDER_U =
      "TException/IoTDBConnectionException occurred when SubscriptionProvider {} heartbeat, set"
      + " SubscriptionProvider unavailable";
  public static final String LOG_IOEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_SERIALIZE_SUBSCRIBE_REQUEST_ARG_5E703B32 = "IOException occurred when SubscriptionProvider {} serialize subscribe request {}";
  public static final String LOG_TEXCEPTION_IOTDBCONNECTIONEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_SUBSCRIBE_REQUEST_ARG_SET_SUBSCRIPTI =
      "TException/IoTDBConnectionException occurred when SubscriptionProvider {} subscribe with"
      + " request {}, set SubscriptionProvider unavailable";
  public static final String LOG_IOEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_SERIALIZE_UNSUBSCRIBE_REQUEST_ARG_85D423CE = "IOException occurred when SubscriptionProvider {} serialize unsubscribe request {}";
  public static final String LOG_TEXCEPTION_IOTDBCONNECTIONEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_UNSUBSCRIBE_REQUEST_ARG_SET_SUBSCRIP =
      "TException/IoTDBConnectionException occurred when SubscriptionProvider {} unsubscribe with"
      + " request {}, set SubscriptionProvider unavailable";
  public static final String LOG_IOEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_SERIALIZE_SEEK_REQUEST_TOPIC_ARG_7620B991 = "IOException occurred when SubscriptionProvider {} serialize seek request for topic {}";
  public static final String LOG_TEXCEPTION_IOTDBCONNECTIONEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_SEEK_REQUEST_TOPIC_ARG_SET_AEAFC363 =
      "TException/IoTDBConnectionException occurred when SubscriptionProvider {} seek with request"
      + " for topic {}, set SubscriptionProvider unavailable";
  public static final String LOG_IOEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_SERIALIZE_SEEK_TOPICPROGRESS_TOPIC_ARG_E64C0FAF = "IOException occurred when SubscriptionProvider {} serialize seek(topicProgress) for topic {}";
  public static final String LOG_TEXCEPTION_IOTDBCONNECTIONEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_SEEK_TOPICPROGRESS_TOPIC_ARG_SET_193 =
      "TException/IoTDBConnectionException occurred when SubscriptionProvider {} seek(topicProgress)"
      + " for topic {}, set SubscriptionProvider unavailable";
  public static final String LOG_IOEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_SERIALIZE_SEEKAFTER_TOPICPROGRESS_TOPIC_ARG_9F79F3CF =
      "IOException occurred when SubscriptionProvider {} serialize seekAfter(topicProgress) for"
      + " topic {}";
  public static final String LOG_TEXCEPTION_IOTDBCONNECTIONEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_SEEKAFTER_TOPICPROGRESS_TOPIC_ARG_SE =
      "TException/IoTDBConnectionException occurred when SubscriptionProvider {}"
      + " seekAfter(topicProgress) for topic {}, set SubscriptionProvider unavailable";
  public static final String LOG_IOEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_SERIALIZE_POLL_REQUEST_ARG_969C4A7A = "IOException occurred when SubscriptionProvider {} serialize poll request {}";
  public static final String LOG_TEXCEPTION_IOTDBCONNECTIONEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_POLL_REQUEST_ARG_SET_SUBSCRIPTIONPRO =
      "TException/IoTDBConnectionException occurred when SubscriptionProvider {} poll with request"
      + " {}, set SubscriptionProvider unavailable";
  public static final String LOG_IOEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_SERIALIZE_COMMIT_REQUEST_ARG_D5335538 = "IOException occurred when SubscriptionProvider {} serialize commit request {}";
  public static final String LOG_TEXCEPTION_IOTDBCONNECTIONEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_COMMIT_REQUEST_ARG_SET_SUBSCRIPTIONP =
      "TException/IoTDBConnectionException occurred when SubscriptionProvider {} commit with request"
      + " {}, set SubscriptionProvider unavailable";
  public static final String LOG_FAILED_COMMIT_DRAINED_PROCESSOR_MESSAGES_CLOSE_4264DB35 = "Failed to commit drained processor messages on close";
  public static final String LOG_FAILED_COMMIT_PENDING_DRAINED_PROCESSOR_MESSAGES_CLOSE_644B5DDD = "Failed to commit pending drained processor messages on close";
  public static final String LOG_SUBSCRIPTIONPULLCONSUMER_ARG_DOES_NOT_SUBSCRIBE_TOPIC_ARG_F40BE4D1 = "SubscriptionPullConsumer {} does not subscribe to topic {}";
  public static final String LOG_SUBSCRIPTIONPULLCONSUMER_ARG_POLL_EMPTY_MESSAGE_TOPICS_ARG_AFTER_ARG_MILLISECOND_78F4D4A6 = "SubscriptionPullConsumer {} poll empty message from topics {} after {} millisecond(s)";
  public static final String EXCEPTION_SUBSCRIPTIONPULLCONSUMER_ARG_CANNOT_SEEK_TOPIC_ARG_SUBSCRIBED_MULTIPLE_TOPICS_BECAUSE_B99BCABC =
      "SubscriptionPullConsumer %s cannot seek topic %s while subscribed to multiple topics because"
      + " processor %s does not support topic-scoped reset";
  public static final String LOG_DETECT_ALREADY_EXISTED_FILE_ARG_POLLING_TOPIC_ARG_RESUME_CONSUMPTION_AD735649 = "Detect already existed file {} when polling topic {}, resume consumption";
  public static final String LOG_DETECT_ALREADY_EXISTED_FILE_ARG_POLLING_TOPIC_ARG_ADD_RANDOM_64BBDEEB = "Detect already existed file {} when polling topic {}, add random suffix {} to filename";
  public static final String LOG_SUBSCRIPTIONRUNTIMECRITICALEXCEPTION_OCCURRED_SUBSCRIPTIONCONSUMER_ARG_POLLING_TOPICS_ARG_C96324AD = "SubscriptionRuntimeCriticalException occurred when SubscriptionConsumer {} polling topics {}";
  public static final String LOG_EXECUTIONEXCEPTION_OCCURRED_SUBSCRIPTIONCONSUMER_ARG_POLLING_TOPICS_ARG_40F5E1CC = "ExecutionException occurred when SubscriptionConsumer {} polling topics {}";
  public static final String LOG_INTERRUPTEDEXCEPTION_OCCURRED_SUBSCRIPTIONCONSUMER_ARG_POLLING_TOPICS_ARG_9B556CD5 = "InterruptedException occurred when SubscriptionConsumer {} polling topics {}";
  public static final String LOG_SUBSCRIPTIONRUNTIMENONCRITICALEXCEPTION_OCCURRED_SUBSCRIPTIONCONSUMER_ARG_POLLING_TOPICS_ARG_61838153 =
      "SubscriptionRuntimeNonCriticalException occurred when SubscriptionConsumer {} polling topics"
      + " {}";
  public static final String LOG_ARG_START_POLL_FILE_ARG_COMMIT_CONTEXT_ARG_AT_OFFSET_0F677E12 = "{} start to poll file {} with commit context {} at offset {}";
  public static final String LOG_SUBSCRIPTIONCONSUMER_ARG_SUCCESSFULLY_POLL_FILE_ARG_COMMIT_CONTEXT_ARG_A23E0FDB = "SubscriptionConsumer {} successfully poll file {} with commit context {}";
  public static final String LOG_ERROR_OCCURRED_SUBSCRIPTIONCONSUMER_ARG_POLLING_FILE_ARG_COMMIT_CONTEXT_ARG_03619D67 =
      "Error occurred when SubscriptionConsumer {} polling file {} with commit context {}: {},"
      + " critical: {}";
  public static final String LOG_ERROR_OCCURRED_SUBSCRIPTIONCONSUMER_ARG_POLLING_TABLETS_COMMIT_CONTEXT_ARG_ARG_FF40B4E1 =
      "Error occurred when SubscriptionConsumer {} polling tablets with commit context {}: {},"
      + " critical: {}";
  public static final String LOG_ARG_FAILED_COMMIT_CONTEXTS_ARG_CF224D1E = "{} failed commit contexts: {}";
  public static final String LOG_ARG_STAGED_ARG_CONSENSUS_ACK_S_REDIRECT_AFTER_PROVIDER_ARG_0F0C0623 = "{} staged {} consensus ack(s) for redirect after provider {} became unavailable";
  public static final String LOG_ARG_KEEP_ARG_NON_CONSENSUS_ACK_S_PENDING_AFTER_PROVIDER_32F56904 = "{} keep {} non-consensus ack(s) pending after provider {} commit failure";
  public static final String LOG_ARG_FAILED_SUBSCRIBE_TOPICS_ARG_SUBSCRIPTION_PROVIDER_ARG_TRY_NEXT_4DF9FC46 =
      "{} failed to subscribe topics {} from subscription provider {}, try next subscription"
      + " provider...";
  public static final String LOG_ARG_FAILED_UNSUBSCRIBE_TOPICS_ARG_SUBSCRIPTION_PROVIDER_ARG_TRY_NEXT_2205E8A8 =
      "{} failed to unsubscribe topics {} from subscription provider {}, try next subscription"
      + " provider...";
  public static final String LOG_ARG_FAILED_SEEK_TOPIC_ARG_SUBSCRIPTION_PROVIDER_ARG_SEEK_REQUIRES_15AFE61D =
      "{} failed to seek topic {} from subscription provider {}; seek requires every provider to"
      + " succeed, so the client will continue notifying the remaining providers before failing this"
      + " seek.";
  public static final String LOG_ARG_FAILED_SEEK_TOPIC_ARG_TOPICPROGRESS_REGIONCOUNT_ARG_PROVIDER_ARG_E999A05B =
      "{} failed to seek topic {} to topicProgress(regionCount={}) from provider {}; seek requires"
      + " every provider to succeed, so the client will continue notifying the remaining providers"
      + " before failing this seek.";
  public static final String LOG_ARG_FAILED_SEEKAFTER_TOPIC_ARG_TOPICPROGRESS_REGIONCOUNT_ARG_PROVIDER_ARG_0C795E87 =
      "{} failed to seekAfter topic {} to topicProgress(regionCount={}) from provider {}; seek"
      + " requires every provider to succeed, so the client will continue notifying the remaining"
      + " providers before failing this seekAfter.";
  public static final String LOG_ARG_FAILED_REDIRECT_ARG_PENDING_CONSENSUS_ACK_S_ARG_VIA_E6A10AC5 = "{} failed to redirect {} pending consensus ack(s) for {} via provider {}";
  public static final String LOG_ARG_FAILED_FETCH_ALL_ENDPOINTS_SUBSCRIPTION_PROVIDER_ARG_TRY_NEXT_25651CAD =
      "{} failed to fetch all endpoints from subscription provider {}, try next subscription"
      + " provider...";
  public static final String EXCEPTION_FAILED_HANDSHAKE_SUBSCRIPTION_PROVIDER_ARG_BECAUSE_ARG_251C2E2A = "Failed to handshake with subscription provider %s because of %s";
  public static final String EXCEPTION_CLUSTER_HAS_NO_AVAILABLE_SUBSCRIPTION_PROVIDERS_ARG_POLL_TOPIC_ARG_FABF7A4E = "Cluster has no available subscription providers when %s poll topic %s";
  public static final String EXCEPTION_SOMETHING_UNEXPECTED_HAPPENED_ARG_POLL_FILE_SUBSCRIPTION_PROVIDER_DATA_NODE_07426483 =
      "something unexpected happened when %s poll file from subscription provider with data node id"
      + " %s, the subscription provider may be unavailable or not existed";
  public static final String EXCEPTION_SOMETHING_UNEXPECTED_HAPPENED_ARG_POLL_TABLETS_SUBSCRIPTION_PROVIDER_DATA_NODE_8D80E611 =
      "something unexpected happened when %s poll tablets from subscription provider with data node"
      + " id %s, the subscription provider may be unavailable or not existed";
  public static final String EXCEPTION_SOMETHING_UNEXPECTED_HAPPENED_ARG_COMMIT_NACK_ARG_MESSAGES_SUBSCRIPTION_PROVIDER_2026D7CE =
      "something unexpected happened when %s commit (nack: %s) messages to subscription provider"
      + " with data node id %s, the subscription provider may be unavailable or not existed";
  public static final String EXCEPTION_CLUSTER_HAS_NO_AVAILABLE_SUBSCRIPTION_PROVIDERS_ARG_SUBSCRIBE_TOPIC_ARG_06E872FE = "Cluster has no available subscription providers when %s subscribe topic %s";
  public static final String EXCEPTION_CLUSTER_HAS_NO_AVAILABLE_SUBSCRIPTION_PROVIDERS_ARG_UNSUBSCRIBE_TOPIC_ARG_BF50B2B1 = "Cluster has no available subscription providers when %s unsubscribe topic %s";
  public static final String EXCEPTION_CLUSTER_HAS_NO_AVAILABLE_SUBSCRIPTION_PROVIDERS_ARG_SEEK_TOPIC_ARG_9F93C2E7 = "Cluster has no available subscription providers when %s seek topic %s";
  public static final String EXCEPTION_CLUSTER_HAS_NO_AVAILABLE_SUBSCRIPTION_PROVIDERS_ARG_SEEKAFTER_TOPIC_ARG_C86D5F85 = "Cluster has no available subscription providers when %s seekAfter topic %s";
  public static final String EXCEPTION_CLUSTER_HAS_NO_AVAILABLE_SUBSCRIPTION_PROVIDERS_ARG_FETCH_ALL_ENDPOINTS_D232693E = "Cluster has no available subscription providers when %s fetch all endpoints";
  public static final String EXCEPTION_PASSWORD_ENCRYPTEDPASSWORD_MUTUALLY_EXCLUSIVE_ENCRYPTEDPASSWORD_ALREADY_SET_E4548A43 = "password and encryptedPassword are mutually exclusive; encryptedPassword is already set";
  public static final String EXCEPTION_PASSWORD_ENCRYPTEDPASSWORD_MUTUALLY_EXCLUSIVE_PASSWORD_ALREADY_SET_BB20AD1E = "password and encryptedPassword are mutually exclusive; password is already set";
  public static final String EXCEPTION_CONSENSUS_MODE_TOPIC_SHOULD_NOT_GENERATE_PIPE_SOURCE_ATTRIBUTES_BBDFF732 = "Consensus mode topic should not generate pipe source attributes";
  public static final String EXCEPTION_UNSUPPORTED_SUBSCRIPTIONCOMMITCONTEXT_VERSION_8021B27B = "Unsupported SubscriptionCommitContext version: ";
  public static final String OUTDATED_SUBSCRIPTION_EVENT = "outdated subscription event";
  public static final String FIELD_SEPARATOR = ", ";
  public static final String DIALECT_NOT_NULL = "dialect";
  public static final String EXCEPTION_TOPIC_ATTRIBUTES_SHOULD_NOT_BE_EMPTY_IN_ALTER_TOPIC_573B2F09 =
      "Topic attributes should not be empty in ALTER TOPIC.";
  public static final String EXCEPTION_TOPIC_OWNER_ATTRIBUTES_SHOULD_BE_MODIFIED_BY_ALTERTOPICOWNER_ONLY_FBD794F4 =
      "Topic owner attributes should be modified by alterTopicOwner only.";
  public static final String EXCEPTION_TOPIC_OWNER_ID_SHOULD_NOT_BE_EMPTY_94976B6D =
      "Topic owner id should not be empty.";

}
