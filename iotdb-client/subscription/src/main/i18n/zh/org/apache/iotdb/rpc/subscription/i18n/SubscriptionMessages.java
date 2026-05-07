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

  private SubscriptionMessages() {}
}
