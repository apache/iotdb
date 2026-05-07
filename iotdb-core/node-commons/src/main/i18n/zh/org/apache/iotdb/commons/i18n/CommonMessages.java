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

public final class CommonMessages {

  // --- startup / shutdown ---
  public static final String STARTUP_FAILED = "启动 [%s] 失败，原因：[%s]";

  // --- path ---
  public static final String ILLEGAL_PATH = "%s 不是合法路径";
  public static final String ILLEGAL_PATH_WITH_REASON = "%s 不是合法路径，原因：%s";
  public static final String PATH_NOT_MEASUREMENT = "该路径不代表一个测点";
  public static final String PATH_DUPLICATED = "路径重复：%s";
  public static final String OBJECT_TYPE_COLUMN_NOT_SUPPORTED = "不支持 object 类型的列。";

  // --- cluster ---
  public static final String NODE_TYPE_NOT_EXIST = "NodeType %s 不存在。";
  public static final String NODE_STATUS_NOT_EXIST = "NodeStatus %s 不存在。";
  public static final String UNKNOWN_NODE_STATUS = "未知 NodeStatus %s。";

  // --- consensus ---
  public static final String UNRECOGNIZED_CONSENSUS_GROUP_ID =
      "无法识别的 ConsensusGroupId：%s";
  public static final String IOTV2_BG_NOT_TERMINATED =
      "IoTV2 后台服务在 {}s 内未终止";
  public static final String IOTV2_BG_STILL_RUNNING =
      "IoTV2 后台线程在 30s 后仍未退出";

  // --- cq ---
  public static final String UNKNOWN_TIMEOUT_POLICY = "未知 TimeoutPolicy：%s";
  public static final String UNKNOWN_CQ_STATE = "未知 CQState：%s";

  // --- memory ---
  public static final String MEMORY_ALLOC_INTERRUPTED =
      "exactAllocate：等待可用内存时被中断";
  public static final String MEMORY_RELEASE_FAILED =
      "releaseWithOutNotify：关闭内存块 {} 失败";
  public static final String MEMORY_RELEASE_FAILED_NO_DETAIL =
      "releaseWithOutNotify：关闭内存块失败";
  public static final String MEMORY_SIZE_SHOULD_BE_POSITIVE =
      "getOrCreateMemoryManager {}：sizeInBytes 应为正数";

  // --- file ---
  public static final String SHOULD_NEVER_TOUCH_HERE = "不应执行到此处";

  // --- externalservice ---
  public static final String UNKNOWN_SERVICE_TYPE = "未知 ServiceType：%s";
  public static final String UNKNOWN_STATE = "未知 State：%s";

  // --- subscription ---
  public static final String CONFIG_PRINT = "{}：{}";

  // --- concurrent ---
  public static final String EXCEPTION_IN_THREAD = "线程 {}-{} 中发生异常";
  public static final String INTERRUPTED_WHILE_AWAITING = "等待条件时被中断";
  public static final String EXCEPTION_WHILE_EVALUATING = "计算条件时发生异常";
  public static final String UNKNOWN_THREAD_NAME = "未知线程名：{}";
  public static final String TASK_CANCELLED_IN_POOL = "线程池 {} 中的任务已取消";
  public static final String EXCEPTION_IN_THREAD_POOL = "线程池 {} 中发生异常";
  public static final String SCHEDULE_TASK_FAILED = "调度任务失败";
  public static final String RUN_THREAD_FAILED = "运行线程失败";

  // --- enums ---
  public static final String SYSTEM_READ_ONLY = "系统模式已设为只读（READ_ONLY）";
  public static final String UNRECOVERABLE_ERROR = "发生不可恢复的错误！直接关闭系统。";

  // --- udf ---
  public static final String UNKNOWN_FUNCTION_TYPE = "未知 FunctionType：%s";
  public static final String INVALID_INPUT = "无效输入：%s";
  public static final String UDF_LIB_ROOT = "UDF lib 根目录：{}";
  public static final String UDF_MD5_READ_ERROR = "读取 {} 的 md5 时出错";
  public static final String VALUE_NOT_NUMERIC = "输入时间序列的值不是数值类型。\n";
  public static final String FAIL_GET_DATA_TYPE = "获取第 %s 行的数据类型失败";
  public static final String UDTF_ABS_SET_TRANSFORMER = "UDTFAbs#setTransformer()";
  public static final String BASE_VALUE_SHOULD_NOT_BE_NULL =
      "比较时基准值不应为 null";
  public static final String SIZE_MUST_BE_POSITIVE = "Size 必须大于 0";

  // --- sync ---
  public static final String UNEXPECTED_SERIALIZATION_ERROR =
      "序列化 PipeInfo 时发生意外错误。";

  // --- security ---
  public static final String ENCRYPT_PASSWORD_ERROR = "加密密码时出错。";
  public static final String CLASSLOADER_NOT_DETERMINED = "无法确定用于加载类的 ClassLoader。";

  private CommonMessages() {}

  public static final String COLLECTION_MUST_NOT_BE_NULL = "集合不能为空。";
  public static final String MAP_MUST_NOT_BE_NULL = "Map 不能为空。";
  public static final String MAP_ENTRY_MUST_NOT_BE_NULL = "Map 条目不能为空。";
  public static final String ITERATOR_MUST_NOT_BE_NULL = "迭代器不能为空";
  public static final String ITERATOR_REMOVE_ONLY_AFTER_NEXT = "Iterator remove() 只能在 next() 之后调用一次";
  public static final String FAIL_TO_GET_DATA_TYPE_IN_ROW = "获取行中数据类型失败，行时间：";
}
