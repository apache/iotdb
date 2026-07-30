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

package org.apache.iotdb.pipe.api.i18n;

public final class PipeApiMessages {

  // --- PipeParameterValidator ---
  public static final String PARAMETER_SHOULD_BE_SET = "参数 %s 必须设置。";
  public static final String EXCEPTION_CANNOT_SPECIFY_BOTH_ARG_AND_ARG_AT_THE_SAME_TIME_7DA8858B =
      "不能同时指定 %s 和 %s";
  public static final String
      EXCEPTION_INVALID_VALUE_ARG_OF_ARG_THE_VALUE_SHOULD_BE_ONE_OF_ARG_7D1B4AF8 =
          "%s 的值 %s 无效。该值应为 %s 之一";

  // --- PipeAttributeNotProvidedException ---
  public static final String EXCEPTION_ATTRIBUTE_ARG_IS_REQUIRED_BUT_WAS_NOT_PROVIDED_5A6C1F93 =
      "属性 \"%s\" 是必填的，但未提供。";

  private PipeApiMessages() {}
}
