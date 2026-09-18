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

package org.apache.iotdb.db.i18n;

public final class DataNodeQueryMessages {

  private DataNodeQueryMessages() {}

  public static final String RESULT_SET_COLUMN_MEMORY_SHORTAGE_EQUIVALENT =
      "本次失败的内存申请超出可用内存，按已记录列的平均内存估算，至少超出相当于 %,d 列的容量。";

  public static final String
      QUERY_EXCEPTION_THERE_IS_NOT_ENOUGH_MEMORY_FOR_QUERY_S_THE_CONTEXTHOLDER_546CDD02 =
          "Query %s 内存不足，contextHolder 为 %s，当前剩余空闲内存为 %dB，该 context 已预留总内存为 %dB，本次请求内存为 %dB。";
}
