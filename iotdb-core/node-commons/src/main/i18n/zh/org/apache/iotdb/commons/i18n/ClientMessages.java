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

package org.apache.iotdb.commons.i18n;

public final class ClientMessages {

  // ClientManager
  public static final String RETURN_CLIENT_TO_POOL_FAILED =
      "归还客户端 {} 到节点 {} 的连接池失败。";
  public static final String RETURN_CLIENT_NULL_NODE =
      "归还客户端 {} 到连接池失败，因为节点为空。"
          + "这可能导致资源泄漏，请检查您的代码。";
  public static final String CLEAR_CLIENT_POOL_FAILED =
      "清除节点 {} 的所有连接池客户端失败。";

  // ThriftClient
  public static final String EXCEPTION_LEVEL_DETAIL =
      "第 {} 层异常，类名 {}，消息 {}";
  public static final String ROOT_CAUSE_DETAIL =
      "根因消息 {}，本地化消息 {}，";
  public static final String BROKEN_PIPE_CLEAR_CONNECTIONS =
      "发送 RPC 时发生管道断裂错误，需要清除所有之前缓存的连接，错误信息为 {}";

  // Async/Sync clients - invalidation
  public static final String CLIENT_INVALIDATED = "此客户端已被标记为失效";
  public static final String CLIENT_INVALIDATED_WITH_ID =
      "客户端 %d 已被标记为失效";

  // Async clients - readiness check
  public static final String UNEXPECTED_EXCEPTION_IN_CLIENT =
      "客户端 {} 中发生意外异常：{}";
  public static final String UNEXPECTED_EXCEPTION_IN_CLIENT_WITH_MSG =
      "客户端 {} 中发生意外异常，错误信息为 {}";

  // Async clients - timeout management
  public static final String TIMEOUT_ALREADY_SET =
      "此客户端的超时时间已被设置为 {}。"
          + "如需将其设置为 {}，请先调用 recoverTimeout() 方法。";
  public static final String TIMEOUT_NOT_MODIFIED =
      "此客户端的超时时间未被修改，无法重置";

  // AsyncPipeDataTransferServiceClient
  public static final String FAILED_TO_SET_TIMEOUT_DYNAMICALLY =
      "动态设置超时时间失败，改为静态设置";
  public static final String HANDSHAKE_FINISHED = "客户端 {} 握手完成";
  public static final String MANUALLY_CLOSING_TRANSPORT =
      "正在手动关闭传输通道以防止资源泄漏。";
  public static final String METHOD_STATE_RESET =
      "由于管理器未运行，方法状态已被重置。";

  // AsyncThriftClientFactory
  public static final String CANNOT_CREATE_ASYNC_FACTORY =
      "无法创建异步 Thrift 客户端工厂 %s";

  // SyncThriftClientWithErrorHandler
  public static final String ERROR_IN_CALLING_METHOD =
      "调用方法 %s 时出错，原因：%s";

  // BorrowNullClientManagerException
  public static final String CANNOT_BORROW_CLIENT_NULL_NODE =
      "无法为空节点借用客户端";

  // AsyncRequestManager
  public static final String ASYNC_REQUEST_TIMEOUT =
      "{} 超时。重试：{}/{}";
  public static final String ASYNC_REQUEST_INTERRUPTED =
      "{} 被中断。重试：{}/{}";
  public static final String ASYNC_REQUEST_FAILED_AFTER_RETRIES =
      "{} 在 {} 次重试后仍然失败，请求索引：{}";
  public static final String UNSUPPORTED_REQUEST_TYPE =
      "不支持的请求类型 %s，请在 AsyncRequestManager::initActionMapBuilder() 中配置";
  public static final String ASYNC_REQUEST_FAILED_ON_NODE =
      "{} 在节点 {} 上失败，原因 {}，正在进行第 {} 次重试...";

  private ClientMessages() {}
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String LOG_RETURN_CLIENT_ARG_POOL_FAILED_BECAUSE_NODE_NULL_81511014 = "归还客户端 {} 到连接池失败，原因：节点为空。";
  public static final String LOG_MAY_CAUSE_RESOURCE_LEAK_PLEASE_CHECK_YOUR_CODE_DD730191 = "这可能导致资源泄漏，请检查代码。";
  public static final String EXCEPTION_UNSUPPORTED_REQUEST_TYPE_2030CDC7 = "不支持的请求类型 ";
  public static final String EXCEPTION_PLEASE_SET_IT_ASYNCREQUESTMANAGER_INITACTIONMAPBUILDER_0F039A93 = "，请在 AsyncRequestManager::initActionMapBuilder() 中设置";
  public static final String EXCEPTION_ERROR_CALLING_METHOD_C04E5A63 = "调用方法时出错 ";
  public static final String EXCEPTION_BECAUSE_ACD0B1C8 = "，原因： ";
  public static final String LOG_PIPE_CONNECTION_TIMEOUT_ADJUSTED_ARG_MS_ARG_MINS_6D126A53 = "Pipe 连接超时已调整为 {} ms（{} mins）";
  public static final String LOG_BODY_SIZE_REQUEST_TOO_LARGE_REQUEST_WILL_SLICED_ORIGIN_REQ_35E73788 = "请求 body 过大，请求将被切片。Origin req: {}-{}。";
  public static final String LOG_REQUEST_BODY_SIZE_ARG_THRESHOLD_ARG_69B1BE00 = "请求 body 大小: {}，阈值: {}";
  public static final String LOG_FAILED_TRANSFER_SLICE_ORIGIN_REQ_ARG_ARG_RETRY_WHOLE_TRANSFER_E1EA2F41 = "传输 slice 失败。Origin req：{}-{}。重试整个传输。";
  public static final String EXCEPTION_FAILED_TRANSFER_SLICE_ORIGIN_REQ_ARG_ARG_SLICE_INDEX_ARG_7219936C = "无法传输 slice。Origin req: %s-%s，slice index: %d，slice count: %d。原因：%s";
  public static final String MESSAGE_FAILED_TO_HANDLE_ASYNC_REQUEST_ERROR_FOR_REQUEST_TYPE_ARG_ON_NODE_ARG_ARG_3AE293F7 = "处理节点 {} 上请求类型 {} 的异步请求错误失败：{}";

}
