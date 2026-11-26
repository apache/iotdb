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

package org.apache.iotdb.trigger.api.i18n;

public final class TriggerApiMessages {

  // --- TriggerType ---
  public static final String NO_SUCH_TRIGGER_TYPE = "不存在该触发器类型（id: %d）";

  // --- TriggerEvent ---
  public static final String NO_SUCH_TRIGGER_EVENT = "不存在该触发器事件（id: %d）";

  // --- FailureStrategy ---
  public static final String UNSUPPORTED_FAILURE_STRATEGY_TYPE =
      "不支持的 FailureStrategy 类型。";

  private TriggerApiMessages() {}
}
