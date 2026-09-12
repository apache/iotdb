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

package org.apache.iotdb.cli.i18n;

public final class FsParserMessages {
  private FsParserMessages() {}

  public static final String WC_MODE = "wc 仅支持 -c";
  public static final String OUTPUT_FORMAT = "无效的输出格式：%s";
  public static final String EXCLUSIVE_SCOPE = "选项 -d 和 -t 不能同时使用";
  public static final String TAG_MATCH_FILTER = "--tag-match 需要 --tag-filter";
  public static final String TAG_MATCH_VALUE = "无效的 --tag-match 值：%s";
  public static final String TAG_MATCH_COUNT = "--tag-match 至少需要两个 TAG 过滤条件";
  public static final String TAG_FILTER_MATCH = "两个或更多 TAG 过滤条件需要 --tag-match all 或 any";
  public static final String OPTION_VALUE = "选项 %s 的值无效：%s";
  public static final String TAG_OPERATOR = "无效的 --tag-filter 运算符：%s";
  public static final String DUPLICATE_MEASUREMENT = "列 '%s' 被重复指定";
}
