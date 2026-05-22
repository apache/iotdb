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

public final class ConfigMessages {

  // ===================== Generic config-set-to pattern =====================
  public static final String CONFIG_SET_TO = "{} is set to {}.";

  // ===================== CommonConfig: system mode / status =====================
  public static final String FAIL_TO_GET_CANONICAL_PATH = "Fail to get canonical path of {}";
  public static final String SET_SYSTEM_MODE = "Set system mode from {} to {}.";
  public static final String STATUS_CHANGE_TO_READ_ONLY =
      "Change system status to ReadOnly! Only query statements are permitted!";
  public static final String STATUS_CHANGE_TO_REMOVING =
      "Change system status to Removing! The current Node is being removed from cluster!";

  // ===================== CommonConfig: timestamp precision =====================
  public static final String WRONG_TIMESTAMP_PRECISION =
      "Wrong timestamp precision, please set as: ms, us or ns ! Current is: {}";

  // ===================== CommonConfig: pipe timeout overflow =====================
  public static final String PIPE_CONNECTOR_HANDSHAKE_TIMEOUT_TOO_LARGE =
      "Given pipe connector handshake timeout is too large, set to {} ms.";
  public static final String PIPE_AIR_GAP_SINK_TABLET_TIMEOUT_TOO_LARGE =
      "Given pipe air gap sink tablet timeout is too large, set to {} ms.";
  public static final String PIPE_SINK_TRANSFER_TIMEOUT_TOO_LARGE =
      "Given pipe sink transfer timeout is too large, set to {} ms.";

  // ===================== CommonConfig: pipe validation =====================
  public static final String CONFIG_MUST_BE_POSITIVE =
      "{} should be greater than 0, configuring it not to change.";
  public static final String IGNORE_INVALID_CONFIG_MUST_BE_POSITIVE =
      "Ignore invalid {} {}, because it must be greater than 0.";

  // ===================== CommonConfig: audit log (SLF4J {} placeholders) =====================
  public static final String UNSUPPORTED_AUDIT_LOG_OPERATION_TYPE =
      "Unsupported audit log operation type: {}";
  public static final String UNSUPPORTED_AUDIT_LOG_OPERATION_LEVEL =
      "Unsupported audit log operation level: {}";

  // ===================== CommonConfig: audit log (String.format %s placeholders) ==============
  public static final String UNSUPPORTED_AUDIT_LOG_OPERATION_TYPE_EX =
      "Unsupported audit log operation type: %s";
  public static final String UNSUPPORTED_AUDIT_LOG_OPERATION_LEVEL_EX =
      "Unsupported audit log operation level: %s";

  // ===================== ConfigurationFileUtils =====================
  public static final String FAILED_TO_UPDATE_APPLIED_PROPERTIES =
      "Failed to update applied properties";
  public static final String FAILED_TO_READ_CONFIGURATION_TEMPLATE =
      "Failed to read configuration template";
  public static final String UPDATING_CONFIGURATION_FILE = "Updating configuration file {}";
  public static final String WAITING_TO_ACQUIRE_CONFIG_FILE_LOCK =
      "Waiting for {} seconds to acquire configuration file update lock."
          + " There may have been an unexpected interruption in the last"
          + " configuration file update. Ignore temporary file {}";

  private ConfigMessages() {}
}
