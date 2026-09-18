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

public final class StorageMessages {

  private StorageMessages() {}

  // ======================== Directory ========================

  public static final String FAILED_TO_DEREGISTER_FILE_LOCK =
      "Failed to deregister file lock because {}";
  public static final String STORAGE_EXCEPTION_UNABLE_TO_CREATE_DIRECTORY_S_BECAUSE_THERE_IS_FILE_UNDER_1C59ACFC =
      "Unable to create directory %s because there is file under the path, please check "
          + "configuration and restart.";
  public static final String STORAGE_EXCEPTION_UNABLE_TO_CREATE_DIRECTORY_S_PLEASE_CHECK_CONFIGURATION_BA580B67 =
      "Unable to create directory %s, please check configuration and restart.";
  public static final String STORAGE_EXCEPTION_CONFLICT_IS_DETECTED_IN_DIRECTORY_S_WHICH_MAY_BE_BEING_USED_CB5C77FC =
      "Conflict is detected in directory %s, which may be being used by another IoTDB "
          + "(ProcessId=%s). Please check configuration and restart.";
}