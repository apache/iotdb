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
}
