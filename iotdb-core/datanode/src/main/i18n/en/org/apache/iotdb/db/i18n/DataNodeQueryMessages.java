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

  public static final String EXCEPTION_FAILED_TO_CONVERT_TSFILE_ARG_TO_TABLETS_STATUS_ARG_E7C9F477 =
      "Failed to convert TsFile %s to tablets. Status: %s";
  public static final String EXCEPTION_FAILED_TO_CONVERT_TSFILE_ARG_TO_TABLETS_ARG_96358FDD =
      "Failed to convert TsFile %s to tablets: %s";
  public static final String MESSAGE_TABLET_CONVERSION_RETURNED_NO_STATUS_88BAB5D3 =
      "Tablet conversion returned no status";
  public static final String
      EXCEPTION_FAILED_TO_LOAD_TSFILES_AFTER_TABLET_CONVERSION_DETAILS_ARG_53647EC5 =
          "Failed to load TsFiles after tablet conversion. Details: %s";
  public static final String
      LOG_LOAD_REJECTED_UUID_ARG_REGION_ARG_ENDPOINT_ARG_STATUS_ARG_8ED0679E =
          "LOAD rejected: uuid={}, region={}, endpoint={}, status={}";
  public static final String LOG_LOAD_COMMAND_REJECTED_UUID_ARG_ENDPOINT_ARG_STATUS_ARG_E8D0031C =
      "LOAD command rejected: uuid={}, endpoint={}, status={}";
  public static final String
      MESSAGE_TARGET_NODE_REJECTED_LOAD_WITHOUT_A_STATUS_OR_A_MESSAGE_F4E4C645 =
          "Target node rejected LOAD without a status or a message";
  public static final String
      MESSAGE_FAILED_TO_DISPATCH_LOAD_ARG_FOR_REGION_ARG_TO_NODE_ARG_ARG_57EFEC3A =
          "Failed to dispatch LOAD %s for region %s to node %s: %s";
  public static final String MESSAGE_FAILED_TO_DISPATCH_LOAD_COMMAND_ARG_TO_NODE_ARG_ARG_53D2BF0B =
      "Failed to dispatch LOAD command %s to node %s: %s";

  public static final String RESULT_SET_COLUMN_MEMORY_SHORTAGE_EQUIVALENT =
      "The failed memory reservation exceeds available memory by the equivalent of at least "
          + "%,d columns, estimated from the observed average column size. ";

  public static final String
      QUERY_EXCEPTION_THERE_IS_NOT_ENOUGH_MEMORY_FOR_QUERY_S_THE_CONTEXTHOLDER_546CDD02 =
          "There is not enough memory for Query %s, the contextHolder is %s,current remaining free "
              + "memory is %dB, already reserved memory for this context in total is %dB, the memory "
              + "requested this time is %dB";
}
