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

package org.apache.iotdb.commons.conf;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** The ten commercial security features gated off in the IoTDB edition. */
public enum IoTDBGatedFeature {
  SEPARATION_OF_POWERS("Separation of powers", "enable_separation_of_powers"),
  TDE("Transparent data encryption", "encrypt_type"),
  FILE_ENCRYPTION(
      "Config/permission file encryption",
      "enable_encrypt_config_file",
      "enable_encrypt_permission_file"),
  INTERNAL_SSL("Internal communication encryption", "enable_internal_ssl"),
  SECURE_ERASE("Secure erase", "enable_secure_erase"),
  PASSWORD_EXPIRATION(
      "Password expiration", "password_expiration_days", "password_expiration_seconds"),
  BRUTE_FORCE(
      "Anti password brute-force", "failed_login_attempts", "failed_login_attempts_per_user"),
  WHITE_BLACK_LIST("IP white/black list", "enable_white_list", "enable_black_list"),
  CONNECTION_LIMIT("Per-user connection limit"),
  IDLE_EVICTION("Idle connection eviction", "idle_session_timeout_in_minutes");

  private final String displayName;
  private final List<String> configKeys;

  IoTDBGatedFeature(String displayName, String... configKeys) {
    this.displayName = displayName;
    this.configKeys = Collections.unmodifiableList(Arrays.asList(configKeys));
  }

  public String getDisplayName() {
    return displayName;
  }

  public List<String> getConfigKeys() {
    return configKeys;
  }
}
