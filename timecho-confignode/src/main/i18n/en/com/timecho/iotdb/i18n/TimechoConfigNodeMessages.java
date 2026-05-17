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

package com.timecho.iotdb.i18n;

/** Compile-time i18n constants for TimechoDB ConfigNode subsystems (English). */
public final class TimechoConfigNodeMessages {

  private TimechoConfigNodeMessages() {}

  // ConfigNode startup
  public static final String HARDWARE_GENERATION_FAILED = "hardware generation failed.";

  // CLI activation
  public static final String CLI_ACTIVATION_SUCCESS =
      "[CLI activation] Successfully updated all ConfigNodes' license";
  public static final String CLI_ACTIVATION_PREFIX = "[CLI activation]";

  // Regulate (license)
  public static final String ACTIVE_NODE_WATCHING_SERVICE_LAUNCHED =
      "Active node watching service launched successfully";
  public static final String EXPIRATION_WARNING_SERVICE_LAUNCHED =
      "Expiration warning service launched successfully";
  public static final String SUCCESSFULLY_CREATE_ACTIVATION_DIR =
      "successfully create activation dir at {}";
  public static final String LICENSE_FILE_DETECTED_DURING_STARTING =
      "License file detected during ConfigNode's starting.";
  public static final String LICENSE_FILE_NOT_DETECTED_DURING_STARTING =
      "License file not detected during ConfigNode's starting.";
  public static final String START_LICENSE_FILE_MONITOR_FAIL = "start licenseFileMonitor fail";
  public static final String LICENSE_FILE_WATCHING_SERVICE_LAUNCHED =
      "License file watching service launched successfully";
  public static final String LICENSE_FILE_CREATION_DETECTED = "license file creation detected";
  public static final String LICENSE_FILE_MODIFICATION_DETECTED = "license file modification detected";
  public static final String LICENSE_FILE_DELETION_DETECTED = "license file deletion detected";
  public static final String SLEEPING_WAS_INTERRUPTED = "Sleeping was interrupted";
  public static final String THIS_LICENSE_NOT_ALLOWED_TO_ACTIVATE_THIS_CONFIGNODE =
      "This license is not allowed to activate this ConfigNode.";
  public static final String LOAD_LICENSE_SUCCESS = "Load license success.";
  public static final String LOAD_LICENSE_FAIL = "Load license fail.";
  public static final String LOADING_LICENSE_WITH_CONTENT = "Loading license: \n{}";
  public static final String LICENSE_VERSION_NOT_SUPPORTED = "license version ";
  public static final String LICENSE_VERSION_NOT_SUPPORTED_SUFFIX = " is not supported";
  public static final String LOADING_REMOTE_LICENSE = "Loading remote license...";
  public static final String LOADING_REMOTE_LICENSE_FAIL = "Loading remote license fail.";
  public static final String LICENSE_UPDATED_BECAUSE_RECEIVE_REMOTE_LICENSE =
      "License updated because receive remote license";
  public static final String LICENSE_HAS_BEEN_GIVEN_UP_BECAUSE =
      "License has been given up, because {}";

  // System info file
  public static final String SYSTEM_INFO_FILE_GENERATED_SUCCESSFULLY =
      "{} file generated successfully. Content is {}";
  public static final String SYSTEM_INFO_FILE_GENERATED_FAIL = "{} file generated fail.";
  public static final String CANNOT_REMOVE_VERSION_FROM_SYSTEM_INFO =
      "Cannot remove version from system info ";
  public static final String GENERATE_SYSTEM_INFO_CONTENT_FAIL = "generate system info content fail";
  public static final String LICENSE_SYSTEM_INFO_VERSION_UNSUPPORTED =
      "License system info version {} is unsupported";
  public static final String VERIFY_SYSTEM_INFO_FAIL = "Verify system info fail.";

  // License file CRUD
  public static final String SET_LICENSE_FILE_SUCCESS = "set license file success: {}";
  public static final String SET_LICENSE_FILE_FAIL = "set license file {} fail";
  public static final String DELETE_LICENSE_FILE_FAIL = "delete license file {} fail: {}";
  public static final String DELETE_LICENSE_FILE = "delete license file：{}";
  public static final String READ_LICENSE_FILE_FAIL = "read license file {} fail: {}";
}
