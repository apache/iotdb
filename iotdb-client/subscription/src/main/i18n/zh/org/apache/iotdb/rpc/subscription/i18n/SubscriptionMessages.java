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
  public static final String PUSH_CONSUMER_POLL_EMPTY_MESSAGE =
      "SubscriptionPushConsumer {} 从主题 {} 拉取到空消息，耗时 {} 毫秒，连续空拉取次数：{}";
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
  public static final String PULL_CONSUMER_POLL_EMPTY_MESSAGE =
      "SubscriptionPullConsumer {} 从主题 {} 拉取到空消息，耗时 {} 毫秒，连续空拉取次数：{}";
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

  // --- TopicMeta (owner fencing) ---
  public static final String OWNER_ID_SHOULD_NOT_BE_EMPTY = "订阅 topic 的 owner id 不应为空";
  public static final String OWNER_EPOCH_SHOULD_NOT_BE_NEGATIVE = "订阅 topic 的 owner epoch 不应为负数";
  public static final String OWNER_EPOCH_SHOULD_INCREASE_MONOTONICALLY =
      "订阅 topic 的 owner epoch 应单调递增，当前 epoch 为 %s，传入 epoch 为 %s";
  public static final String OWNER_LEASE_DURATION_SHOULD_BE_POSITIVE =
      "订阅 topic 的 owner 租约时长应为正数，topic：%s，owner-lease-duration-ms：%s";
  public static final String OWNER_EPOCH_SHOULD_NEVER_BE_REUSED =
      "订阅 topic 的 owner epoch 不应被复用，topic：%s，已授予的最大 owner-epoch：%s，传入 owner-id：%s，传入"
          + " owner-epoch：%s";
  public static final String OWNER_SHOULD_NOT_BE_CLEARED_BY_STALE_META =
      "订阅 topic 的 owner 不应被过期的 topic meta 清除，topic：%s，当前 owner-id：%s，当前 owner-epoch：%s";
  public static final String OWNER_EPOCH_SHOULD_NOT_ROLL_BACK =
      "订阅 topic 的 owner epoch 不应回退，topic：%s，当前 owner-id：%s，当前 owner-epoch：%s，传入"
          + " owner-id：%s，传入 owner-epoch：%s";
  public static final String OWNER_EPOCH_SHOULD_BE_SET_WHEN_OWNER_ID_SET =
      "当设置 %s 时，订阅 topic 的 owner epoch 也应被设置";
  public static final String OWNER_LEASE_DURATION_SHOULD_BE_POSITIVE_WHEN_SET =
      "当设置 %s 时，订阅 topic 的 owner 租约时长应为正数";

  private SubscriptionMessages() {}
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String EXCEPTION_UNEXPECTED_FIELDS_ARG_WAS_OBTAINED_DURING_SHOW_TOPIC_30B5D702 = "执行 SHOW TOPIC 时获取到意外字段 %s...";
  public static final String EXCEPTION_UNEXPECTED_FIELDS_ARG_WAS_OBTAINED_DURING_SHOW_SUBSCRIPTION_71F9C549 = "执行 SHOW SUBSCRIPTION 时获取到意外字段 %s...";
  public static final String EXCEPTION_SUBSCRIPTION_SESSION_MUST_CONFIGURED_ENDPOINT_2CAD53A9 = "订阅会话必须配置 endpoint。";
  public static final String EXCEPTION_ARG_ILLEGAL_IDENTIFIER_NOT_ENCLOSED_BACKTICKS_CAN_ONLY_CONSIST_DIGITS_55D6A31F =
      "%s 非法，未用反引号括起的标识符只能包含数字、字符和下划线。";
  public static final String EXCEPTION_WATERMARK_TIMESTAMP_ONLY_AVAILABLE_WATERMARK_MESSAGES_ACTUAL_MESSAGE_TYPE_F8E32C57 = "Watermark 时间戳仅适用于 watermark 消息，实际消息类型：";
  public static final String EXCEPTION_ARG_DO_NOT_SUPPORT_GETRESULTSETS_7789852D = "%s 不支持 getResultSets().";
  public static final String EXCEPTION_ARG_DO_NOT_SUPPORT_GETRECORDTABLETITERATOR_46B4A489 = "%s 不支持 getRecordTabletIterator().";
  public static final String EXCEPTION_ARG_DO_NOT_SUPPORT_GETTSFILE_40D23462 = "%s 不支持 getTsFile().";
  public static final String EXCEPTION_USER_DATA_HAS_BEEN_REMOVED_ARG_7093644B = "已从 %s 移除用户数据。";
  public static final String EXCEPTION_ARG_DOES_NOT_SUPPORT_OPENTREEREADER_TABLE_MODEL_TSFILE_68621ECF = "%s 不支持对表模型 TsFile 调用 openTreeReader()。";
  public static final String EXCEPTION_ARG_DOES_NOT_SUPPORT_OPENTABLEREADER_TREE_MODEL_TSFILE_CEC27860 = "%s 不支持对树模型 TsFile 调用 openTableReader()。";
  public static final String EXCEPTION_UNKNOWN_COLUMN_CATEGORY_4F49F64B = "未知的列 category: ";
  public static final String EXCEPTION_DATA_TYPE_ARG_NOT_SUPPORTED_31213160 = "不支持的数据类型 %s。";
  public static final String LOG_SUBSCRIPTIONPUSHCONSUMER_ARG_POLL_EMPTY_MESSAGE_TOPICS_ARG_AFTER_ARG_MILLISECOND_741801C2 = "SubscriptionPushConsumer {} 从主题 {} poll 到空消息，耗时 {} 毫秒";
  public static final String LOG_CONSUMER_LISTENER_RAISED_EXCEPTION_CONSUMING_MESSAGE_ARG_867EE46D = "消费者 listener 消费消息时抛出异常：{}";
  public static final String LOG_ARG_FAILED_CREATE_CONNECTION_ARG_BECAUSE_ARG_E536E22A = "{} 无法与 {} 创建连接，原因：{}";
  public static final String LOG_ARG_FAILED_FETCH_ALL_ENDPOINTS_ARG_BECAUSE_ARG_2C9E11D4 = "{} 无法从 {} 获取所有 endpoint，原因：{}";
  public static final String LOG_TERMINATION_OCCURRED_SUBSCRIPTIONCONSUMER_ARG_POLLING_TOPICS_UNSUBSCRIBE_TOPIC_ARG_AUTOMATICALLY_E9B695FA = "订阅消费者 {} poll 主题时发生终止，自动取消订阅主题 {}";
  public static final String LOG_ARG_FAILED_SENDING_HEARTBEAT_SUBSCRIPTION_PROVIDER_ARG_BECAUSE_ARG_SET_0B38FB1F = "{} 无法向 SubscriptionProvider {} 发送心跳，原因：{}，将 SubscriptionProvider 设为不可用";
  public static final String LOG_EXCEPTION_OCCURRED_ARG_CLOSING_REMOVING_SUBSCRIPTION_PROVIDER_ARG_BECAUSE_ARG_2EC38739 = "{} 关闭并移除 SubscriptionProvider {} 时发生异常，原因：{}";
  public static final String EXCEPTION_CLUSTER_HAS_NO_AVAILABLE_SUBSCRIPTION_PROVIDERS_CONNECT_INITIAL_ENDPOINTS_ARG_5DB83198 = "集群没有可连接初始 endpoint %s 的可用 SubscriptionProvider";
  public static final String LOG_ARG_HAS_BEEN_LAUNCHED_SET_CORE_POOL_SIZE_ARG_WILL_0FDECBE3 = "{} 已启动，将 core pool size 设置为 {} 的操作会被忽略";
  public static final String LOG_INTERRUPT_WORKER_WHICH_MAY_CAUSE_SOME_TASK_INCONSISTENT_PLEASE_CHECK_04926D9F = "中断 worker 可能造成某些任务不一致。请检查业务日志。";
  public static final String LOG_THREAD_POOL_CAN_T_SHUTDOWN_EVEN_INTERRUPTING_WORKER_THREADS_WHICH_A49166F9 =
      "即使中断工作线程也无法关闭线程池，可能造成某些任务不一致。请检查业务日志。";
  public static final String LOG_CURRENT_THREAD_INTERRUPTED_IT_TRYING_STOP_WORKER_THREADS_MAY_LEAVE_CF07ABA0 =
      "当前线程尝试停止工作线程时被中断，可能留下不一致状态。请检查业务日志。";
  public static final String LOG_IOEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_SERIALIZE_HANDSHAKE_REQUEST_ARG_C1414290 = "SubscriptionProvider {} 序列化握手请求 {} 时发生 IOException";
  public static final String LOG_TEXCEPTION_IOTDBCONNECTIONEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_HANDSHAKE_REQUEST_ARG_SET_SUBSCRIPTI = "SubscriptionProvider {} 使用请求 {} 握手时发生 TException/IoTDBConnectionException，将 SubscriptionProvider 设为不可用";
  public static final String LOG_TEXCEPTION_IOTDBCONNECTIONEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_CLOSE_SET_SUBSCRIPTIONPROVIDER_UNAVA = "SubscriptionProvider {} 关闭时发生 TException/IoTDBConnectionException，将 SubscriptionProvider 设为不可用";
  public static final String LOG_IOEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_SERIALIZE_HEARTBEAT_REQUEST_ARG_634C8333 = "SubscriptionProvider {} 序列化心跳请求 {} 时发生 IOException";
  public static final String LOG_TEXCEPTION_IOTDBCONNECTIONEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_HEARTBEAT_SET_SUBSCRIPTIONPROVIDER_U = "SubscriptionProvider {} 发送心跳时发生 TException/IoTDBConnectionException，将 SubscriptionProvider 设为不可用";
  public static final String LOG_IOEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_SERIALIZE_SUBSCRIBE_REQUEST_ARG_5E703B32 = "SubscriptionProvider {} 序列化订阅请求 {} 时发生 IOException";
  public static final String LOG_TEXCEPTION_IOTDBCONNECTIONEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_SUBSCRIBE_REQUEST_ARG_SET_SUBSCRIPTI = "SubscriptionProvider {} 使用请求 {} 订阅时发生 TException/IoTDBConnectionException，将 SubscriptionProvider 设为不可用";
  public static final String LOG_IOEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_SERIALIZE_UNSUBSCRIBE_REQUEST_ARG_85D423CE = "SubscriptionProvider {} 序列化取消订阅请求 {} 时发生 IOException";
  public static final String LOG_TEXCEPTION_IOTDBCONNECTIONEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_UNSUBSCRIBE_REQUEST_ARG_SET_SUBSCRIP = "SubscriptionProvider {} 使用请求 {} 取消订阅时发生 TException/IoTDBConnectionException，将 SubscriptionProvider 设为不可用";
  public static final String LOG_IOEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_SERIALIZE_SEEK_REQUEST_TOPIC_ARG_7620B991 = "SubscriptionProvider {} 序列化主题 {} 的 seek 请求时发生 IOException";
  public static final String LOG_TEXCEPTION_IOTDBCONNECTIONEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_SEEK_REQUEST_TOPIC_ARG_SET_AEAFC363 = "SubscriptionProvider {} 对主题 {} 执行 seek 请求时发生 TException/IoTDBConnectionException，将 SubscriptionProvider 设为不可用";
  public static final String LOG_IOEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_SERIALIZE_SEEK_TOPICPROGRESS_TOPIC_ARG_E64C0FAF = "SubscriptionProvider {} 序列化主题 {} 的 seek(TopicProgress) 时发生 IOException";
  public static final String LOG_TEXCEPTION_IOTDBCONNECTIONEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_SEEK_TOPICPROGRESS_TOPIC_ARG_SET_193 = "SubscriptionProvider {} 对主题 {} 执行 seek(TopicProgress) 时发生 TException/IoTDBConnectionException，将 SubscriptionProvider 设为不可用";
  public static final String LOG_IOEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_SERIALIZE_SEEKAFTER_TOPICPROGRESS_TOPIC_ARG_9F79F3CF = "SubscriptionProvider {} 序列化主题 {} 的 seekAfter(TopicProgress) 时发生 IOException";
  public static final String LOG_TEXCEPTION_IOTDBCONNECTIONEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_SEEKAFTER_TOPICPROGRESS_TOPIC_ARG_SE = "SubscriptionProvider {} 对主题 {} 执行 seekAfter(TopicProgress) 时发生 TException/IoTDBConnectionException，将 SubscriptionProvider 设为不可用";
  public static final String LOG_IOEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_SERIALIZE_POLL_REQUEST_ARG_969C4A7A = "SubscriptionProvider {} 序列化 poll 请求 {} 时发生 IOException";
  public static final String LOG_TEXCEPTION_IOTDBCONNECTIONEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_POLL_REQUEST_ARG_SET_SUBSCRIPTIONPRO = "SubscriptionProvider {} 使用请求 {} poll 时发生 TException/IoTDBConnectionException，将 SubscriptionProvider 设为不可用";
  public static final String LOG_IOEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_SERIALIZE_COMMIT_REQUEST_ARG_D5335538 = "SubscriptionProvider {} 序列化 commit 请求 {} 时发生 IOException";
  public static final String LOG_TEXCEPTION_IOTDBCONNECTIONEXCEPTION_OCCURRED_SUBSCRIPTIONPROVIDER_ARG_COMMIT_REQUEST_ARG_SET_SUBSCRIPTIONP = "SubscriptionProvider {} 使用请求 {} commit 时发生 TException/IoTDBConnectionException，将 SubscriptionProvider 设为不可用";
  public static final String LOG_FAILED_COMMIT_DRAINED_PROCESSOR_MESSAGES_CLOSE_4264DB35 = "关闭时无法提交已排空的 processor 消息";
  public static final String LOG_FAILED_COMMIT_PENDING_DRAINED_PROCESSOR_MESSAGES_CLOSE_644B5DDD = "关闭时无法提交待处理且已排空的 processor 消息";
  public static final String LOG_SUBSCRIPTIONPULLCONSUMER_ARG_DOES_NOT_SUBSCRIBE_TOPIC_ARG_F40BE4D1 = "SubscriptionPullConsumer {} 未订阅主题 {}";
  public static final String LOG_SUBSCRIPTIONPULLCONSUMER_ARG_POLL_EMPTY_MESSAGE_TOPICS_ARG_AFTER_ARG_MILLISECOND_78F4D4A6 = "SubscriptionPullConsumer {} 从主题 {} poll 到空消息，耗时 {} 毫秒";
  public static final String EXCEPTION_SUBSCRIPTIONPULLCONSUMER_ARG_CANNOT_SEEK_TOPIC_ARG_SUBSCRIBED_MULTIPLE_TOPICS_BECAUSE_B99BCABC =
      "SubscriptionPullConsumer %s 订阅多个主题时无法 seek 主题 %s，原因：processor %s 不支持"
      + "主题范围的 reset";
  public static final String LOG_DETECT_ALREADY_EXISTED_FILE_ARG_POLLING_TOPIC_ARG_RESUME_CONSUMPTION_AD735649 = "检测到已存在文件 {}，poll 主题 {} 时恢复消费";
  public static final String LOG_DETECT_ALREADY_EXISTED_FILE_ARG_POLLING_TOPIC_ARG_ADD_RANDOM_64BBDEEB = "检测到已存在文件 {}，poll 主题 {} 时为文件名添加随机后缀 {}";
  public static final String LOG_SUBSCRIPTIONRUNTIMECRITICALEXCEPTION_OCCURRED_SUBSCRIPTIONCONSUMER_ARG_POLLING_TOPICS_ARG_C96324AD = "SubscriptionConsumer {} poll 主题 {} 时发生 SubscriptionRuntimeCriticalException";
  public static final String LOG_EXECUTIONEXCEPTION_OCCURRED_SUBSCRIPTIONCONSUMER_ARG_POLLING_TOPICS_ARG_40F5E1CC = "SubscriptionConsumer {} poll 主题 {} 时发生 ExecutionException";
  public static final String LOG_INTERRUPTEDEXCEPTION_OCCURRED_SUBSCRIPTIONCONSUMER_ARG_POLLING_TOPICS_ARG_9B556CD5 = "SubscriptionConsumer {} poll 主题 {} 时发生 InterruptedException";
  public static final String LOG_SUBSCRIPTIONRUNTIMENONCRITICALEXCEPTION_OCCURRED_SUBSCRIPTIONCONSUMER_ARG_POLLING_TOPICS_ARG_61838153 = "SubscriptionConsumer {} poll 主题 {} 时发生 SubscriptionRuntimeNonCriticalException";
  public static final String LOG_ARG_START_POLL_FILE_ARG_COMMIT_CONTEXT_ARG_AT_OFFSET_0F677E12 = "{} 开始 poll 文件 {}，commit context {}，offset {}";
  public static final String LOG_SUBSCRIPTIONCONSUMER_ARG_SUCCESSFULLY_POLL_FILE_ARG_COMMIT_CONTEXT_ARG_A23E0FDB = "订阅消费者 {} 成功 poll 文件 {}，commit context {}";
  public static final String LOG_ERROR_OCCURRED_SUBSCRIPTIONCONSUMER_ARG_POLLING_FILE_ARG_COMMIT_CONTEXT_ARG_03619D67 = "订阅消费者 {} poll 文件 {} 时发生错误，commit context {}：{}，critical：{}";
  public static final String LOG_ERROR_OCCURRED_SUBSCRIPTIONCONSUMER_ARG_POLLING_TABLETS_COMMIT_CONTEXT_ARG_ARG_FF40B4E1 = "订阅消费者 {} poll tablets 时发生错误，commit context {}：{}，critical：{}";
  public static final String LOG_ARG_FAILED_COMMIT_CONTEXTS_ARG_CF224D1E = "{} commit contexts 失败：{}";
  public static final String LOG_ARG_STAGED_ARG_CONSENSUS_ACK_S_REDIRECT_AFTER_PROVIDER_ARG_0F0C0623 = "{} 已暂存 {} 个 consensus ack，用于 provider {} 不可用后的重定向";
  public static final String LOG_ARG_KEEP_ARG_NON_CONSENSUS_ACK_S_PENDING_AFTER_PROVIDER_32F56904 = "{} 保留 {} 个 non-consensus ack 待处理，因为 provider {} commit 失败";
  public static final String LOG_ARG_FAILED_SUBSCRIBE_TOPICS_ARG_SUBSCRIPTION_PROVIDER_ARG_TRY_NEXT_4DF9FC46 = "{} 无法订阅主题 {}，SubscriptionProvider {} 失败，尝试下一个 SubscriptionProvider...";
  public static final String LOG_ARG_FAILED_UNSUBSCRIBE_TOPICS_ARG_SUBSCRIPTION_PROVIDER_ARG_TRY_NEXT_2205E8A8 = "{} 无法取消订阅主题 {}，SubscriptionProvider {} 失败，尝试下一个 SubscriptionProvider...";
  public static final String LOG_ARG_FAILED_SEEK_TOPIC_ARG_SUBSCRIPTION_PROVIDER_ARG_SEEK_REQUIRES_15AFE61D =
      "{} 无法在主题 {} 上从 SubscriptionProvider {} 执行 seek; seek 要求每个 provider 都成功，"
      + "因此客户端会先继续通知剩余 provider，然后再使本次 seek 失败。";
  public static final String LOG_ARG_FAILED_SEEK_TOPIC_ARG_TOPICPROGRESS_REGIONCOUNT_ARG_PROVIDER_ARG_E999A05B =
      "{} 无法在主题 {} 上 seek 到 TopicProgress(regionCount={})，provider {} 失败; seek 要求每个"
      + " provider 都成功，因此客户端会先继续通知剩余 provider，然后再使本次 seek 失败。";
  public static final String LOG_ARG_FAILED_SEEKAFTER_TOPIC_ARG_TOPICPROGRESS_REGIONCOUNT_ARG_PROVIDER_ARG_0C795E87 =
      "{} 无法在主题 {} 上 seekAfter 到 TopicProgress(regionCount={})，provider {} 失败; seek 要求每个"
      + " provider 都成功，因此客户端会先继续通知剩余 provider，然后再使本次 seekAfter 失败。";
  public static final String LOG_ARG_FAILED_REDIRECT_ARG_PENDING_CONSENSUS_ACK_S_ARG_VIA_E6A10AC5 = "{} 无法重定向 {} 个待处理 consensus ack，目标 {}，provider {}";
  public static final String LOG_ARG_FAILED_FETCH_ALL_ENDPOINTS_SUBSCRIPTION_PROVIDER_ARG_TRY_NEXT_25651CAD = "{} 无法从 SubscriptionProvider {} 获取所有 endpoint，尝试下一个 SubscriptionProvider...";
  public static final String EXCEPTION_FAILED_HANDSHAKE_SUBSCRIPTION_PROVIDER_ARG_BECAUSE_ARG_251C2E2A = "与 SubscriptionProvider %s 握手失败，原因：%s";
  public static final String EXCEPTION_CLUSTER_HAS_NO_AVAILABLE_SUBSCRIPTION_PROVIDERS_ARG_POLL_TOPIC_ARG_FABF7A4E = "%s poll 主题 %s 时，集群没有可用的 SubscriptionProvider";
  public static final String EXCEPTION_SOMETHING_UNEXPECTED_HAPPENED_ARG_POLL_FILE_SUBSCRIPTION_PROVIDER_DATA_NODE_07426483 =
      "%s 从 DataNode id 为 %s 的 SubscriptionProvider poll 文件时发生意外情况，该 SubscriptionProvider"
      + " 可能不可用或不存在";
  public static final String EXCEPTION_SOMETHING_UNEXPECTED_HAPPENED_ARG_POLL_TABLETS_SUBSCRIPTION_PROVIDER_DATA_NODE_8D80E611 =
      "%s 从 DataNode id 为 %s 的 SubscriptionProvider poll tablets 时发生意外情况，该 SubscriptionProvider"
      + " 可能不可用或不存在";
  public static final String EXCEPTION_SOMETHING_UNEXPECTED_HAPPENED_ARG_COMMIT_NACK_ARG_MESSAGES_SUBSCRIPTION_PROVIDER_2026D7CE =
      "%s 向 SubscriptionProvider commit 消息（nack：%s），DataNode id 为 %s，"
      + "该 SubscriptionProvider 可能不可用或不存在";
  public static final String EXCEPTION_CLUSTER_HAS_NO_AVAILABLE_SUBSCRIPTION_PROVIDERS_ARG_SUBSCRIBE_TOPIC_ARG_06E872FE = "%s 订阅主题 %s 时，集群没有可用的 SubscriptionProvider";
  public static final String EXCEPTION_CLUSTER_HAS_NO_AVAILABLE_SUBSCRIPTION_PROVIDERS_ARG_UNSUBSCRIBE_TOPIC_ARG_BF50B2B1 = "%s 取消订阅主题 %s 时，集群没有可用的 SubscriptionProvider";
  public static final String EXCEPTION_CLUSTER_HAS_NO_AVAILABLE_SUBSCRIPTION_PROVIDERS_ARG_SEEK_TOPIC_ARG_9F93C2E7 = "%s seek 主题 %s 时，集群没有可用的 SubscriptionProvider";
  public static final String EXCEPTION_CLUSTER_HAS_NO_AVAILABLE_SUBSCRIPTION_PROVIDERS_ARG_SEEKAFTER_TOPIC_ARG_C86D5F85 = "%s seekAfter 主题 %s 时，集群没有可用的 SubscriptionProvider";
  public static final String EXCEPTION_CLUSTER_HAS_NO_AVAILABLE_SUBSCRIPTION_PROVIDERS_ARG_FETCH_ALL_ENDPOINTS_D232693E = "%s 获取所有 endpoint 时，集群没有可用的 SubscriptionProvider";
  public static final String EXCEPTION_PASSWORD_ENCRYPTEDPASSWORD_MUTUALLY_EXCLUSIVE_ENCRYPTEDPASSWORD_ALREADY_SET_E4548A43 = "password 与 encryptedPassword 互斥；已设置 encryptedPassword";
  public static final String EXCEPTION_PASSWORD_ENCRYPTEDPASSWORD_MUTUALLY_EXCLUSIVE_PASSWORD_ALREADY_SET_BB20AD1E = "password 与 encryptedPassword 互斥；已设置 password";
  public static final String EXCEPTION_CONSENSUS_MODE_TOPIC_SHOULD_NOT_GENERATE_PIPE_SOURCE_ATTRIBUTES_BBDFF732 = "Consensus mode 主题不应生成 pipe source attributes";
  public static final String EXCEPTION_UNSUPPORTED_SUBSCRIPTIONCOMMITCONTEXT_VERSION_8021B27B = "不支持的 SubscriptionCommitContext 版本：";
  public static final String OUTDATED_SUBSCRIPTION_EVENT = "过期的订阅事件";
  public static final String FIELD_SEPARATOR = "，";
  public static final String DIALECT_NOT_NULL = "dialect 不能为空";
  public static final String EXCEPTION_TOPIC_ATTRIBUTES_SHOULD_NOT_BE_EMPTY_IN_ALTER_TOPIC_573B2F09 =
      "ALTER TOPIC 中的 topic attributes 不应为空。";
  public static final String EXCEPTION_TOPIC_OWNER_ATTRIBUTES_SHOULD_BE_MODIFIED_BY_ALTERTOPICOWNER_ONLY_FBD794F4 =
      "topic owner attributes 只能通过 alterTopicOwner 修改。";
  public static final String EXCEPTION_TOPIC_OWNER_ID_SHOULD_NOT_BE_EMPTY_94976B6D =
      "topic owner id 不应为空。";

}
