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
  public static final String PARAMETER_SHOULD_BE_SET = "Parameter %s should be set.";
  public static final String EXCEPTION_CANNOT_SPECIFY_BOTH_ARG_AND_ARG_AT_THE_SAME_TIME_7DA8858B =
      "Cannot specify both %s and %s at the same time";
  public static final String
      EXCEPTION_INVALID_VALUE_ARG_OF_ARG_THE_VALUE_SHOULD_BE_ONE_OF_ARG_7D1B4AF8 =
          "Invalid value %s of %s. The value should be one of %s";

  // --- PipeAttributeNotProvidedException ---
  public static final String EXCEPTION_ATTRIBUTE_ARG_IS_REQUIRED_BUT_WAS_NOT_PROVIDED_5A6C1F93 =
      "Attribute \"%s\" is required but was not provided.";

  private PipeApiMessages() {}
}
