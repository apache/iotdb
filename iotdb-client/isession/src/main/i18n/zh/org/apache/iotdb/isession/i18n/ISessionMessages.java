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

package org.apache.iotdb.isession.i18n;

public final class ISessionMessages {

  // --- SessionDataSet ---
  public static final String OBJECT_TYPE_ONLY_SUPPORT_GET_STRING =
      "OBJECT 类型仅支持 getString";

  // --- Template ---
  public static final String DUPLICATED_CHILD_IN_TEMPLATE =
      "模板中存在重复的子节点。";
  public static final String NOT_DIRECT_CHILD_OF_TEMPLATE =
      "这不是该模板的直接子节点：";

  private ISessionMessages() {}
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String EXCEPTION_DATA_TYPE_ARG_NOT_SUPPORTED_31213160 = "不支持数据类型 %s。";

}
