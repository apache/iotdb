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
  public static final String UTILITY_CLASS = "工具类";

  // --- SubscriptionPollRequest ---
  public static final String UNEXPECTED_REQUEST_TYPE =
      "意外的请求类型：{}，payload 将为 null";

  // --- SubscriptionPollResponse ---
  public static final String UNEXPECTED_RESPONSE_TYPE =
      "意外的响应类型：{}，payload 将为 null";

  // --- IdentifierUtils ---
  public static final String NULL_IDENTIFIER_NOT_SUPPORTED = "不支持 null 标识符";
  public static final String EMPTY_IDENTIFIER_NOT_SUPPORTED = "不支持空标识符";

  // --- PollTimer ---
  public static final String INVALID_NEGATIVE_TIMEOUT = "无效的负超时时间 ";

  // --- AbstractSubscriptionPushConsumer ---
  public static final String PUSH_CONSUMER_CANCEL_AUTO_POLL =
      "SubscriptionPushConsumer {} 取消自动拉取工作线程";
  public static final String PUSH_CONSUMER_SUBMIT_AUTO_POLL =
      "SubscriptionPushConsumer {} 提交自动拉取工作线程";
  public static final String CONSUMER_LISTENER_FAILURE =
      "消费消息时消费者监听器结果失败：{}";
  public static final String AUTO_POLL_UNEXPECTED = "自动拉取消息时发生意外情况...";

  // --- SubscriptionExecutorServiceManager ---
  public static final String EXECUTOR_LAUNCHING = "正在启动 {}，核心线程池大小：{}...";
  public static final String EXECUTOR_SHUTTING_DOWN = "正在关闭 {}...";
  public static final String EXECUTOR_NOT_LAUNCHED_SUBMIT =
      "{} 尚未启动，忽略提交任务";
  public static final String EXECUTOR_NOT_LAUNCHED_INVOKE =
      "{} 尚未启动，忽略批量调用任务";
  public static final String EXECUTOR_NOT_LAUNCHED_ZERO =
      "{} 尚未启动，返回零";
  public static final String EXECUTOR_NOT_LAUNCHED_SCHEDULE =
      "{} 尚未启动，忽略 scheduleWithFixedDelay 任务";

  // --- AbstractSubscriptionProviders ---
  public static final String PROVIDER_CLOSE_FAILED =
      "关闭订阅提供者 {} 失败，原因：{}";
  public static final String ADD_NEW_PROVIDER = "添加新的订阅提供者 {}";
  public static final String CLOSE_STALE_PROVIDER = "关闭并移除过期的订阅提供者 {}";
  public static final String OPEN_PROVIDERS_FAILED =
      "为消费者 {} 打开提供者失败，原因：{}";
  public static final String FETCH_ENDPOINTS_FAILED =
      "为消费者 {} 获取所有端点失败，原因：{}";

  // --- AbstractSubscriptionPullConsumer ---
  public static final String PULL_CONSUMER_CANCEL_AUTO_COMMIT =
      "SubscriptionPullConsumer {} 取消自动提交工作线程";
  public static final String PULL_CONSUMER_SUBMIT_AUTO_COMMIT =
      "SubscriptionPullConsumer {} 提交自动提交工作线程";
  public static final String AUTO_COMMIT_UNEXPECTED =
      "自动提交消息时发生意外情况...";
  public static final String COMMIT_DURING_CLOSE_UNEXPECTED =
      "关闭期间提交消息时发生意外情况";

  // --- AbstractSubscriptionConsumer ---
  public static final String UNEXPECTED_RESPONSE_TYPE_WARN = "意外的响应类型：{}";
  public static final String CONSUMER_CANCEL_HEARTBEAT_WORKER =
      "SubscriptionConsumer {} 取消心跳工作线程";
  public static final String CONSUMER_SUBMIT_HEARTBEAT_WORKER =
      "SubscriptionConsumer {} 提交心跳工作线程";
  public static final String CONSUMER_CANCEL_ENDPOINTS_SYNCER =
      "SubscriptionConsumer {} 取消端点同步器";
  public static final String CONSUMER_SUBMIT_ENDPOINTS_SYNCER =
      "SubscriptionConsumer {} 提交端点同步器";

  private SubscriptionMessages() {}
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String EXCEPTION_UNEXPECTED_FIELDS_ARG_WAS_OBTAINED_DURING_SHOW_TOPIC_30B5D702 = "Unexpected fields %s was obtained during SHOW TOPIC...";
  public static final String EXCEPTION_UNEXPECTED_FIELDS_ARG_WAS_OBTAINED_DURING_SHOW_SUBSCRIPTION_71F9C549 = "Unexpected fields %s was obtained during SHOW SUBSCRIPTION...";
  public static final String EXCEPTION_SUBSCRIPTION_SESSION_MUST_CONFIGURED_ENDPOINT_2CAD53A9 = "订阅 会话 必须be configured with an endpoint.";
  public static final String EXCEPTION_ARG_ILLEGAL_IDENTIFIER_NOT_ENCLOSED_BACKTICKS_CAN_ONLY_CONSIST_DIGITS_55D6A31F =
      "%s is illegal, identifier 不en关闭d with backticks can only consist of digits, characters and"
      + " underscore.";
  public static final String EXCEPTION_WATERMARK_TIMESTAMP_ONLY_AVAILABLE_WATERMARK_MESSAGES_ACTUAL_MESSAGE_TYPE_F8E32C57 = "Watermark timestamp is only available for watermark 消息s, actual 消息 type: ";
  public static final String EXCEPTION_ARG_DO_NOT_SUPPORT_GETRESULTSETS_7789852D = "%s 不support getResultSets().";
  public static final String EXCEPTION_ARG_DO_NOT_SUPPORT_GETRECORDTABLETITERATOR_46B4A489 = "%s 不support getRecordTabletIterator().";
  public static final String EXCEPTION_ARG_DO_NOT_SUPPORT_GETTSFILE_40D23462 = "%s 不support getTs文件().";
  public static final String EXCEPTION_USER_DATA_HAS_BEEN_REMOVED_ARG_7093644B = "用户 data 已移除d from %s.";
  public static final String EXCEPTION_ARG_DOES_NOT_SUPPORT_OPENTREEREADER_TABLE_MODEL_TSFILE_68621ECF = "%s 不support 打开Tree读取er() for table model ts文件.";
  public static final String EXCEPTION_ARG_DOES_NOT_SUPPORT_OPENTABLEREADER_TREE_MODEL_TSFILE_CEC27860 = "%s 不support 打开Table读取er() for tree model ts文件.";
  public static final String EXCEPTION_UNKNOWN_COLUMN_CATEGORY_4F49F64B = "未知的column category: ";
  public static final String EXCEPTION_DATA_TYPE_ARG_NOT_SUPPORTED_31213160 = "Data type %s 不是supported.";
  public static final String LOG_SUBSCRIPTIONPUSHCONSUMER_ARG_POLL_EMPTY_MESSAGE_TOPICS_ARG_AFTER_ARG_MILLISECOND_741801C2 = "订阅Push消费者 {} poll 为空 消息 from 主题s {} after {} millisecond(s)";
  public static final String LOG_CONSUMER_LISTENER_RAISED_EXCEPTION_CONSUMING_MESSAGE_ARG_867EE46D = "消费者 listener raised an 异常 while consuming 消息: {}";
  public static final String LOG_ARG_FAILED_CREATE_CONNECTION_ARG_BECAUSE_ARG_E536E22A = "{} 无法创建 连接 with {}，原因：of {}";
  public static final String LOG_ARG_FAILED_FETCH_ALL_ENDPOINTS_ARG_BECAUSE_ARG_2C9E11D4 = "{} 无法fetch all endpoints from {}，原因：of {}";
  public static final String LOG_TERMINATION_OCCURRED_SUBSCRIPTIONCONSUMER_ARG_POLLING_TOPICS_UNSUBSCRIBE_TOPIC_ARG_AUTOMATICALLY_E9B695FA = "Termination occurred when 订阅消费者 {} polling 主题s, unsubscribe 主题 {} automatically";
  public static final String LOG_ARG_FAILED_SENDING_HEARTBEAT_SUBSCRIPTION_PROVIDER_ARG_BECAUSE_ARG_SET_0B38FB1F = "{} 无法sending heartbeat to 订阅 provider {}，原因：of {}, set 订阅 provider unavailable";
  public static final String LOG_EXCEPTION_OCCURRED_ARG_CLOSING_REMOVING_SUBSCRIPTION_PROVIDER_ARG_BECAUSE_ARG_2EC38739 = "当{} closing and removing 订阅 provider {}，原因：of {}";
  public static final String EXCEPTION_CLUSTER_HAS_NO_AVAILABLE_SUBSCRIPTION_PROVIDERS_CONNECT_INITIAL_ENDPOINTS_ARG_5DB83198 = "Cluster has no available 订阅 providers to connect with initial endpoints %s";
  public static final String LOG_ARG_HAS_BEEN_LAUNCHED_SET_CORE_POOL_SIZE_ARG_WILL_0FDECBE3 = "{} 已launched, set core pool size to {} will be ignored";
  public static final String LOG_INTERRUPT_WORKER_WHICH_MAY_CAUSE_SOME_TASK_INCONSISTENT_PLEASE_CHECK_04926D9F = "Interrupt the worker, which may cause some task inconsistent. Please check the biz logs.";
  public static final String LOG_THREAD_POOL_CAN_T_SHUTDOWN_EVEN_INTERRUPTING_WORKER_THREADS_WHICH_A49166F9 =
      "Th读取 pool can't be shutdown even with interrupting worker th读取s, which may cause some task"
      + " inconsistent. Please check the biz logs.";
  public static final String LOG_CURRENT_THREAD_INTERRUPTED_IT_TRYING_STOP_WORKER_THREADS_MAY_LEAVE_CF07ABA0 =
      "The 当前th读取 is interrupted when it is trying to 停止 the worker th读取s. This may leave an"
      + " inconsistent state. Please check the biz logs.";
  public static final String LOG_IOEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_SERIALIZE_HANDSHAKE_REQUEST_ARG_C1414290 = "IO当订阅Provider {} 序列化 handshake request {}";
  public static final String LOG_TEXCEPTION_IOTDBCONNECTIONEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_HANDSHAKE_REQUEST_ARG_SET_SUBSCRIPTI = "T异常/IoTDB连接当订阅Provider {} handshake with request {}, set 订阅Provider unavailable";
  public static final String LOG_TEXCEPTION_IOTDBCONNECTIONEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_CLOSE_SET_SUBSCRIPTIONPROVIDER_UNAVA = "T异常/IoTDB连接当订阅Provider {} 关闭, set 订阅Provider unavailable";
  public static final String LOG_IOEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_SERIALIZE_HEARTBEAT_REQUEST_ARG_634C8333 = "IO当订阅Provider {} 序列化 heartbeat request {}";
  public static final String LOG_TEXCEPTION_IOTDBCONNECTIONEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_HEARTBEAT_SET_SUBSCRIPTIONPROVIDER_U = "T异常/IoTDB连接当订阅Provider {} heartbeat, set 订阅Provider unavailable";
  public static final String LOG_IOEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_SERIALIZE_SUBSCRIBE_REQUEST_ARG_5E703B32 = "IO当订阅Provider {} 序列化 subscribe request {}";
  public static final String LOG_TEXCEPTION_IOTDBCONNECTIONEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_SUBSCRIBE_REQUEST_ARG_SET_SUBSCRIPTI = "T异常/IoTDB连接当订阅Provider {} subscribe with request {}, set 订阅Provider unavailable";
  public static final String LOG_IOEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_SERIALIZE_UNSUBSCRIBE_REQUEST_ARG_85D423CE = "IO当订阅Provider {} 序列化 unsubscribe request {}";
  public static final String LOG_TEXCEPTION_IOTDBCONNECTIONEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_UNSUBSCRIBE_REQUEST_ARG_SET_SUBSCRIP = "T异常/IoTDB连接当订阅Provider {} unsubscribe with request {}, set 订阅Provider unavailable";
  public static final String LOG_IOEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_SERIALIZE_SEEK_REQUEST_TOPIC_ARG_7620B991 = "IO当订阅Provider {} 序列化 seek request for 主题 {}";
  public static final String LOG_TEXCEPTION_IOTDBCONNECTIONEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_SEEK_REQUEST_TOPIC_ARG_SET_AEAFC363 = "T异常/IoTDB连接当订阅Provider {} seek with request for 主题 {}, set 订阅Provider unavailable";
  public static final String LOG_IOEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_SERIALIZE_SEEK_TOPICPROGRESS_TOPIC_ARG_E64C0FAF = "IO当订阅Provider {} 序列化 seek(主题Progress) for 主题 {}";
  public static final String LOG_TEXCEPTION_IOTDBCONNECTIONEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_SEEK_TOPICPROGRESS_TOPIC_ARG_SET_193 = "T异常/IoTDB连接当订阅Provider {} seek(主题Progress) for 主题 {}, set 订阅Provider unavailable";
  public static final String LOG_IOEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_SERIALIZE_SEEKAFTER_TOPICPROGRESS_TOPIC_ARG_9F79F3CF = "IO当订阅Provider {} 序列化 seekAfter(主题Progress) for 主题 {}";
  public static final String LOG_TEXCEPTION_IOTDBCONNECTIONEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_SEEKAFTER_TOPICPROGRESS_TOPIC_ARG_SE = "T异常/IoTDB连接当订阅Provider {} seekAfter(主题Progress) for 主题 {}, set 订阅Provider unavailable";
  public static final String LOG_IOEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_SERIALIZE_POLL_REQUEST_ARG_969C4A7A = "IO当订阅Provider {} 序列化 poll request {}";
  public static final String LOG_TEXCEPTION_IOTDBCONNECTIONEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_POLL_REQUEST_ARG_SET_SUBSCRIPTIONPRO = "T异常/IoTDB连接当订阅Provider {} poll with request {}, set 订阅Provider unavailable";
  public static final String LOG_IOEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_SERIALIZE_COMMIT_REQUEST_ARG_D5335538 = "IO当订阅Provider {} 序列化 commit request {}";
  public static final String LOG_TEXCEPTION_IOTDBCONNECTIONEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_COMMIT_REQUEST_ARG_SET_SUBSCRIPTIONP = "T异常/IoTDB连接当订阅Provider {} commit with request {}, set 订阅Provider unavailable";
  public static final String LOG_FAILED_COMMIT_DRAINED_PROCESSOR_MESSAGES_CLOSE_4264DB35 = "无法commit drained processor 消息s on 关闭";
  public static final String LOG_FAILED_COMMIT_PENDING_DRAINED_PROCESSOR_MESSAGES_CLOSE_644B5DDD = "无法commit pending drained processor 消息s on 关闭";
  public static final String LOG_SUBSCRIPTIONPULLCONSUMER_ARG_DOES_NOT_SUBSCRIBE_TOPIC_ARG_F40BE4D1 = "订阅Pull消费者 {} 不subscribe to 主题 {}";
  public static final String LOG_SUBSCRIPTIONPULLCONSUMER_ARG_POLL_EMPTY_MESSAGE_TOPICS_ARG_AFTER_ARG_MILLISECOND_78F4D4A6 = "订阅Pull消费者 {} poll 为空 消息 from 主题s {} after {} millisecond(s)";
  public static final String EXCEPTION_SUBSCRIPTIONPULLCONSUMER_ARG_CANNOT_SEEK_TOPIC_ARG_SUBSCRIBED_MULTIPLE_TOPICS_BECAUSE_B99BCABC =
      "订阅Pull消费者 %s cannot seek 主题 %s while subscribed to multiple 主题s，原因：processor %s 不support"
      + " 主题-scoped reset";
  public static final String LOG_DETECT_ALREADY_EXISTED_FILE_ARG_POLLING_TOPIC_ARG_RESUME_CONSUMPTION_AD735649 = "Detect 已经existed 文件 {} when polling 主题 {}, resume consumption";
  public static final String LOG_DETECT_ALREADY_EXISTED_FILE_ARG_POLLING_TOPIC_ARG_ADD_RANDOM_64BBDEEB = "Detect 已经existed 文件 {} when polling 主题 {}, add random suffix {} to 文件name";
  public static final String LOG_SUBSCRIPTIONRUNTIMECRITICALEXCEPTION_OCCURRED_SUBSCRIPTIONCONSUMER_ARG_POLLING_TOPICS_ARG_C96324AD = "订阅RuntimeCritical当订阅消费者 {} polling 主题s {}";
  public static final String LOG_EXECUTIONEXCEPTION_OCCURRED_SUBSCRIPTIONCONSUMER_ARG_POLLING_TOPICS_ARG_40F5E1CC = "Execution当订阅消费者 {} polling 主题s {}";
  public static final String LOG_INTERRUPTEDEXCEPTION_OCCURRED_SUBSCRIPTIONCONSUMER_ARG_POLLING_TOPICS_ARG_9B556CD5 = "Interrupted当订阅消费者 {} polling 主题s {}";
  public static final String LOG_SUBSCRIPTIONRUNTIMENONCRITICALEXCEPTION_OCCURRED_SUBSCRIPTIONCONSUMER_ARG_POLLING_TOPICS_ARG_61838153 = "订阅RuntimeNonCritical当订阅消费者 {} polling 主题s {}";
  public static final String LOG_ARG_START_POLL_FILE_ARG_COMMIT_CONTEXT_ARG_AT_OFFSET_0F677E12 = "{} 开始 to poll 文件 {} with commit context {} at offset {}";
  public static final String LOG_SUBSCRIPTIONCONSUMER_ARG_SUCCESSFULLY_POLL_FILE_ARG_COMMIT_CONTEXT_ARG_A23E0FDB = "订阅消费者 {} 成功fully poll 文件 {} with commit context {}";
  public static final String LOG_ERROR_OCCURRED_SUBSCRIPTIONCONSUMER_ARG_POLLING_FILE_ARG_COMMIT_CONTEXT_ARG_03619D67 = "当订阅消费者 {} polling 文件 {} with commit context {}: {}, critical: {}";
  public static final String LOG_ERROR_OCCURRED_SUBSCRIPTIONCONSUMER_ARG_POLLING_TABLETS_COMMIT_CONTEXT_ARG_ARG_FF40B4E1 = "当订阅消费者 {} polling tablets with commit context {}: {}, critical: {}";
  public static final String LOG_ARG_FAILED_COMMIT_CONTEXTS_ARG_CF224D1E = "{} 失败 commit contexts: {}";
  public static final String LOG_ARG_STAGED_ARG_CONSENSUS_ACK_S_REDIRECT_AFTER_PROVIDER_ARG_0F0C0623 = "{} staged {} consensus ack(s) for redirect after provider {} became unavailable";
  public static final String LOG_ARG_KEEP_ARG_NON_CONSENSUS_ACK_S_PENDING_AFTER_PROVIDER_32F56904 = "{} keep {} non-consensus ack(s) pending after provider {} commit failure";
  public static final String LOG_ARG_FAILED_SUBSCRIBE_TOPICS_ARG_SUBSCRIPTION_PROVIDER_ARG_TRY_NEXT_4DF9FC46 = "{} 无法subscribe 主题s {} from 订阅 provider {}, try next 订阅 provider...";
  public static final String LOG_ARG_FAILED_UNSUBSCRIBE_TOPICS_ARG_SUBSCRIPTION_PROVIDER_ARG_TRY_NEXT_2205E8A8 = "{} 无法unsubscribe 主题s {} from 订阅 provider {}, try next 订阅 provider...";
  public static final String LOG_ARG_FAILED_SEEK_TOPIC_ARG_SUBSCRIPTION_PROVIDER_ARG_SEEK_REQUIRES_15AFE61D =
      "{} 无法seek 主题 {} from 订阅 provider {}; seek requires every provider to 成功, so the client will"
      + " continue notifying the remaining providers before failing this seek.";
  public static final String LOG_ARG_FAILED_SEEK_TOPIC_ARG_TOPICPROGRESS_REGIONCOUNT_ARG_PROVIDER_ARG_E999A05B =
      "{} 无法seek 主题 {} to 主题Progress(regionCount={}) from provider {}; seek requires every provider"
      + " to 成功, so the client will continue notifying the remaining providers before failing this"
      + " seek.";
  public static final String LOG_ARG_FAILED_SEEKAFTER_TOPIC_ARG_TOPICPROGRESS_REGIONCOUNT_ARG_PROVIDER_ARG_0C795E87 =
      "{} 无法seekAfter 主题 {} to 主题Progress(regionCount={}) from provider {}; seek requires every"
      + " provider to 成功, so the client will continue notifying the remaining providers before failing"
      + " this seekAfter.";
  public static final String LOG_ARG_FAILED_REDIRECT_ARG_PENDING_CONSENSUS_ACK_S_ARG_VIA_E6A10AC5 = "{} 无法redirect {} pending consensus ack(s) for {} via provider {}";
  public static final String LOG_ARG_FAILED_FETCH_ALL_ENDPOINTS_SUBSCRIPTION_PROVIDER_ARG_TRY_NEXT_25651CAD = "{} 无法fetch all endpoints from 订阅 provider {}, try next 订阅 provider...";
  public static final String EXCEPTION_FAILED_HANDSHAKE_SUBSCRIPTION_PROVIDER_ARG_BECAUSE_ARG_251C2E2A = "无法handshake with 订阅 provider %s，原因：of %s";
  public static final String EXCEPTION_CLUSTER_HAS_NO_AVAILABLE_SUBSCRIPTION_PROVIDERS_ARG_POLL_TOPIC_ARG_FABF7A4E = "Cluster has no available 订阅 providers when %s poll 主题 %s";
  public static final String EXCEPTION_SOMETHING_UNEXPECTED_HAPPENED_ARG_POLL_FILE_SUBSCRIPTION_PROVIDER_DATA_NODE_07426483 =
      "something unexpected happened when %s poll 文件 from 订阅 provider with data 节点 id %s, the 订阅"
      + " provider may be unavailable or 不existed";
  public static final String EXCEPTION_SOMETHING_UNEXPECTED_HAPPENED_ARG_POLL_TABLETS_SUBSCRIPTION_PROVIDER_DATA_NODE_8D80E611 =
      "something unexpected happened when %s poll tablets from 订阅 provider with data 节点 id %s, the"
      + " 订阅 provider may be unavailable or 不existed";
  public static final String EXCEPTION_SOMETHING_UNEXPECTED_HAPPENED_ARG_COMMIT_NACK_ARG_MESSAGES_SUBSCRIPTION_PROVIDER_2026D7CE =
      "something unexpected happened when %s commit (nack: %s) 消息s to 订阅 provider with data 节点 id"
      + " %s, the 订阅 provider may be unavailable or 不existed";
  public static final String EXCEPTION_CLUSTER_HAS_NO_AVAILABLE_SUBSCRIPTION_PROVIDERS_ARG_SUBSCRIBE_TOPIC_ARG_06E872FE = "Cluster has no available 订阅 providers when %s subscribe 主题 %s";
  public static final String EXCEPTION_CLUSTER_HAS_NO_AVAILABLE_SUBSCRIPTION_PROVIDERS_ARG_UNSUBSCRIBE_TOPIC_ARG_BF50B2B1 = "Cluster has no available 订阅 providers when %s unsubscribe 主题 %s";
  public static final String EXCEPTION_CLUSTER_HAS_NO_AVAILABLE_SUBSCRIPTION_PROVIDERS_ARG_SEEK_TOPIC_ARG_9F93C2E7 = "Cluster has no available 订阅 providers when %s seek 主题 %s";
  public static final String EXCEPTION_CLUSTER_HAS_NO_AVAILABLE_SUBSCRIPTION_PROVIDERS_ARG_SEEKAFTER_TOPIC_ARG_C86D5F85 = "Cluster has no available 订阅 providers when %s seekAfter 主题 %s";
  public static final String EXCEPTION_CLUSTER_HAS_NO_AVAILABLE_SUBSCRIPTION_PROVIDERS_ARG_FETCH_ALL_ENDPOINTS_D232693E = "Cluster has no available 订阅 providers when %s fetch all endpoints";
  public static final String EXCEPTION_PASSWORD_ENCRYPTEDPASSWORD_MUTUALLY_EXCLUSIVE_ENCRYPTEDPASSWORD_ALREADY_SET_E4548A43 = "password and encryptedPassword are mutually exclusive; encryptedPassword is 已经set";
  public static final String EXCEPTION_PASSWORD_ENCRYPTEDPASSWORD_MUTUALLY_EXCLUSIVE_PASSWORD_ALREADY_SET_BB20AD1E = "password and encryptedPassword are mutually exclusive; password is 已经set";
  public static final String EXCEPTION_CONSENSUS_MODE_TOPIC_SHOULD_NOT_GENERATE_PIPE_SOURCE_ATTRIBUTES_BBDFF732 = "Consensus mode 主题 应not generate pipe source attributes";
  public static final String EXCEPTION_UNSUPPORTED_SUBSCRIPTIONCOMMITCONTEXT_VERSION_8021B27B = "不支持的订阅CommitContext version: ";
  public static final String OUTDATED_SUBSCRIPTION_EVENT = "过期的订阅事件";
  public static final String FIELD_SEPARATOR = "，";
  public static final String DIALECT_NOT_NULL = "dialect 不能为空";

}
