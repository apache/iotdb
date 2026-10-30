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

public final class DataNodePipeMessages {

  private DataNodePipeMessages() {}

  // ===================== LEGACY PIPE RECEIVER RESPONSES =====================

  public static final String MESSAGE_PIPE_DATA_TRANSPORT_ERROR_ARG_ECE6A238 =
      "Pipe data transport error, %s";
  public static final String MESSAGE_FAIL_TO_LOAD_PIPEDATA_BECAUSE_ARG_49526D0C =
      "Fail to load pipeData because %s";
  public static final String MESSAGE_FAILED_TO_RESOLVE_LEGACY_PIPE_RECEIVER_FILE_ARG_ARG_9B82CF3F =
      "Failed to resolve legacy pipe receiver file %s: %s";
  public static final String
      MESSAGE_FAILED_TO_APPEND_LEGACY_PIPE_RECEIVER_FILE_ARG_AT_OFFSET_ARG_WITH_ARG_BYTES_ARG_57428A06 =
          "Failed to append legacy pipe receiver file %s at offset %d with %d bytes: %s";

  public static final String
      LOG_THIS_CONNECTOR_ONLY_SUPPORT_PIPEINSERTNODETABLETINSERTIONEVENT_AND_PIPERAWTABLETINSERTIONEVENT_IGNORE_ARG_9D8780C0 =
          "This Connector only support PipeInsertNodeTabletInsertionEvent and PipeRawTabletInsertionEvent. Ignore {}.";
}
