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

package org.apache.iotdb.rpc.i18n;

public final class RpcMessages {

  // TElasticFramedTransport - FrameError
  public static final String FRAME_ERROR_HTTP_REQUEST =
      "检测到异常帧大小 (%d)，可能正在发送 HTTP GET/POST%s 请求到 Thrift-RPC 端口，请确认使用了正确的端口";
  public static final String FRAME_ERROR_TLS_REQUEST =
      "检测到异常帧大小 (%d)，可能正在发送 TLS ClientHello 请求%s"
          + "到未启用 SSL 的 Thrift-RPC 端口，请确认使用了正确的配置";
  public static final String FRAME_ERROR_NEGATIVE_FRAME_SIZE = "读取到负数帧大小 (%d)%s！";
  public static final String FRAME_ERROR_FRAME_SIZE_EXCEEDED =
      "帧大小 (%d) 超过保护最大值 (%d)%s！";
  public static final String FRAME_ERROR_STRING_LENGTH_EXCEEDED =
      "字符串长度 (%d) 超过保护最大值 (%d)%s！";

  // TElasticFramedTransport - SSL
  public static final String NON_SSL_TO_SSL_PORT =
      "可能正在发送非 SSL 请求%s 到启用了 SSL 的 Thrift-RPC 端口，请确认使用了正确的配置";

  // ConfigurableTByteBuffer
  public static final String UNEXPECTED_END_OF_INPUT = "输入缓冲区意外结束";
  public static final String NOT_ENOUGH_ROOM_IN_OUTPUT = "输出缓冲区空间不足";

  // BaseRpcTransportFactory
  public static final String COULD_NOT_LOAD_KEYSTORE = "无法加载密钥库或信任库文件";

  // AutoResizingBuffer
  public static final String AUTO_RESIZING_BUFFER_ALLOCATE_INTERRUPTED =
      "AutoResizingBuffer 分配 %d 字节内存时被中断";
  public static final String AUTO_RESIZING_BUFFER_ALLOCATE_FAILED =
      "AutoResizingBuffer 在 %d 次重试后仍无法分配 %d 字节内存";

  // IoTDBRpcDataSet / IoTDBJDBCDataSet
  public static final String CLOSE_OPERATION_SERVER_ERROR = "服务端关闭操作失败，原因：";
  public static final String CLOSE_OPERATION_CONNECTION_ERROR = "连接服务端执行关闭操作时出错 ";
  public static final String DATASET_ALREADY_CLOSED = "该数据集已关闭";
  public static final String COLUMN_INDEX_SHOULD_START_FROM_1 = "列索引应从 1 开始";
  public static final String COLUMN_INDEX_OUT_OF_RANGE = "列索引 %d 超出范围 %d";
  public static final String UNKNOWN_COLUMN_NAME = "未知列名：";
  public static final String NO_RECORD_REMAINS = "没有剩余记录";
  public static final String CANNOT_CLOSE_DATASET = "无法关闭数据集，网络连接异常：{} ";

  // RpcUtils
  public static final String UNKNOWN_TIME_PRECISION = "未知时间精度：";
  public static final String UNKNOWN_TIME_FACTOR = "未知时间因子：";

  // PreparedParameterSerde
  public static final String FAILED_TO_SERIALIZE_PARAMETERS = "序列化参数失败";
  public static final String INVALID_PARAMETER_COUNT = "无效的参数数量：";
  public static final String UNSUPPORTED_TYPE = "不支持的类型：";

  // SynchronizedHandler
  public static final String ERROR_IN_CALLING_METHOD = "调用方法时出错：";

  // Shared fragments
  public static final String EMPTY_MESSAGE = "";
  public static final String REMOTE_ADDRESS_PREFIX = " 来自 ";

  private RpcMessages() {}
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String EXCEPTION_SIZE_COLUMNINDEX2TSBLOCKCOLUMNINDEXLIST_ARG_DOESN_T_EQUAL_SIZE_COLUMNNAMELIST_ARG_F1209A2B = "columnIndex2TsBlockColumnIndexList 的大小 %s 不等于 columnNameList 的大小 %s。";
  public static final String EXCEPTION_CANNOT_FETCH_RESULT_SERVER_BECAUSE_NETWORK_CONNECTION_ARG_24BE1326 = "由于网络连接问题，无法从服务器获取结果：{} ";
  public static final String EXCEPTION_ARG_ARG_046AFB8B = "%d: %s";
  public static final String EXCEPTION_LATER_REQUEST_SAME_GROUP_WILL_REDIRECTED_0A61CB0B = "同一组中的后续请求将被重定向到 ";
  public static final String EXCEPTION_NO_SUPPORTED_KEYSTORE_OR_TRUSTSTORE_TYPE_IS_AVAILABLE_B6EA1528 =
      "没有可用的 keystore 或 truststore 类型";
  public static final String EXCEPTION_FAILED_TO_LOAD_KEYSTORE_OR_TRUSTSTORE_FILE_F3306313 =
      "加载 keystore 或 truststore 文件失败";
  public static final String EXCEPTION_KEYSTORE_OR_TRUSTSTORE_FILE_NOT_FOUND_5A6845B2 =
      "未找到 keystore 或 truststore 文件：";

}
