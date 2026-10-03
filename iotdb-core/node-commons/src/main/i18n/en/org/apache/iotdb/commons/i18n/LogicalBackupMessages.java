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

public final class LogicalBackupMessages {

  public static final String EXCEPTION_DIRECTORY_5F8F22B8 = "directory";
  public static final String EXCEPTION_MANIFEST_7F5CB74A = "manifest";
  public static final String EXCEPTION_FSYNC_POLICY_6D493614 = "fsyncPolicy";
  public static final String EXCEPTION_EVENT_GROUP_ID_C6F6268A = "eventGroupId";
  public static final String EXCEPTION_LOGICAL_BACKUP_DIRECTORY_ALREADY_EXISTS_ARG_1C521D1D =
      "Logical backup directory already exists: %s";
  public static final String
      EXCEPTION_LOGICAL_BACKUP_EVENT_MUST_CONTAIN_AT_LEAST_ONE_REQUEST_0C278BAC =
          "Logical backup event must contain at least one request";
  public static final String EXCEPTION_LOGICAL_BACKUP_REQUEST_BODY_MUST_NOT_BE_NULL_EFFD92D9 =
      "Logical backup request body must not be null";
  public static final String EXCEPTION_LOGICAL_BACKUP_RECORD_EXCEEDS_MAX_RECORD_BYTES_B9A9C996 =
      "Logical backup record exceeds max-record-bytes";
  public static final String EXCEPTION_UNSUPPORTED_LOGICAL_BACKUP_MANIFEST_FORMAT_ARG_5005F99E =
      "Unsupported logical backup manifest format: %s";
  public static final String
      EXCEPTION_LOGICAL_BACKUP_MANIFEST_DOES_NOT_MATCH_ARG_EXPECTED_ARG_FOUND_ARG_D7BC8AD1 =
          "Logical backup manifest does not match %s: expected %s, found %s";
  public static final String EXCEPTION_LOGICAL_BACKUP_DIRECTORY_IS_LOCKED_ARG_A4366800 =
      "Logical backup directory is locked: %s";
  public static final String EXCEPTION_LOGICAL_BACKUP_WRITER_IS_CLOSED_EE463BBB =
      "Logical backup writer is closed";
  public static final String EXCEPTION_INCOMPLETE_LOGICAL_BACKUP_SEGMENT_HEADER_ARG_1FE8371A =
      "Incomplete logical backup segment header: %s";
  public static final String EXCEPTION_INVALID_LOGICAL_BACKUP_SEGMENT_FOOTER_SIZE_ARG_4711E278 =
      "Invalid logical backup segment footer size: %s";
  public static final String
      EXCEPTION_INVALID_LOGICAL_BACKUP_RECORD_MAGIC_AT_OFFSET_ARG_IN_ARG_C132B9D5 =
          "Invalid logical backup record magic at offset %d in %s";
  public static final String
      EXCEPTION_LOGICAL_BACKUP_RECORD_HEADER_CRC_MISMATCH_AT_OFFSET_ARG_IN_ARG_7076DD79 =
          "Logical backup record header CRC mismatch at offset %d in %s";
  public static final String
      EXCEPTION_INVALID_LOGICAL_BACKUP_RECORD_LENGTH_AT_OFFSET_ARG_IN_ARG_A7F4048E =
          "Invalid logical backup record length at offset %d in %s";
  public static final String
      EXCEPTION_LOGICAL_BACKUP_RECORD_PAYLOAD_CRC_MISMATCH_AT_OFFSET_ARG_IN_ARG_F9436552 =
          "Logical backup record payload CRC mismatch at offset %d in %s";
  public static final String EXCEPTION_UNKNOWN_LOGICAL_BACKUP_RECORD_TYPE_ARG_IN_ARG_22BB599D =
      "Unknown logical backup record type %d in %s";
  public static final String EXCEPTION_NESTED_LOGICAL_BACKUP_EVENT_GROUPS_IN_ARG_896AC47A =
      "Nested logical backup event groups in %s";
  public static final String EXCEPTION_LOGICAL_BACKUP_COMMIT_WITHOUT_BEGIN_IN_ARG_B9031541 =
      "Logical backup commit without begin in %s";
  public static final String
      EXCEPTION_LOGICAL_BACKUP_OPERATION_INDEX_MISMATCH_AT_OFFSET_ARG_IN_ARG_30C3005F =
          "Logical backup operation index mismatch at offset %d in %s";
  public static final String EXCEPTION_LOGICAL_BACKUP_SEQUENCE_MISMATCH_AT_OFFSET_ARG_IN_ARG_CCEC352B =
      "Logical backup sequence mismatch at offset %d in %s";
  public static final String EXCEPTION_INCOMPLETE_LOGICAL_BACKUP_RECORD_TAIL_ARG_F722DEE7 =
      "Incomplete logical backup record tail: %s";
  public static final String
      EXCEPTION_SEALED_LOGICAL_BACKUP_SEGMENT_CONTAINS_AN_OPEN_EVENT_GROUP_ARG_54C274D8 =
          "Sealed logical backup segment contains an open event group: %s";
  public static final String EXCEPTION_INVALID_LOGICAL_BACKUP_SEGMENT_MAGIC_ARG_8340BA73 =
      "Invalid logical backup segment magic: %s";
  public static final String
      EXCEPTION_UNSUPPORTED_LOGICAL_BACKUP_SEGMENT_MAJOR_VERSION_ARG_ARG_CE07F1CB =
          "Unsupported logical backup segment major version %d: %s";
  public static final String EXCEPTION_LOGICAL_BACKUP_SEGMENT_HEADER_CRC_MISMATCH_ARG_07466D55 =
      "Logical backup segment header CRC mismatch: %s";
  public static final String EXCEPTION_INVALID_LOGICAL_BACKUP_SEGMENT_FOOTER_MAGIC_ARG_D973E2B4 =
      "Invalid logical backup segment footer magic: %s";
  public static final String EXCEPTION_LOGICAL_BACKUP_SEGMENT_FOOTER_ID_MISMATCH_ARG_D832F14D =
      "Logical backup segment footer ID mismatch: %s";
  public static final String EXCEPTION_LOGICAL_BACKUP_SEGMENT_DIGEST_MISMATCH_ARG_85346116 =
      "Logical backup segment digest mismatch: %s";
  public static final String EXCEPTION_LOGICAL_BACKUP_SEGMENT_FOOTER_CRC_MISMATCH_ARG_25FB7D91 =
      "Logical backup segment footer CRC mismatch: %s";
  public static final String
      EXCEPTION_LOGICAL_BACKUP_SEGMENT_FOOTER_METADATA_MISMATCH_ARG_77D1FADB =
          "Logical backup segment footer metadata mismatch: %s";
  public static final String
      EXCEPTION_LOGICAL_BACKUP_EVENT_ID_ARG_WAS_ALREADY_WRITTEN_WITH_A_DIFFERENT_DIGEST_6A297330 =
          "Logical backup event ID %s was already written with a different digest";
  public static final String EXCEPTION_INVALID_LOGICAL_BACKUP_WRITER_CONFIGURATION_707BCC99 =
      "Invalid logical backup writer configuration: segment-size, max-record, fsync-batch and"
          + " fsync-period must be positive";
  public static final String EXCEPTION_LOGICAL_BACKUP_REQUEST_OUTSIDE_EVENT_GROUP_8351818F =
      "Logical backup request outside an event group at offset %d in %s";
  public static final String EXCEPTION_LOGICAL_BACKUP_CONTROL_RECORD_INSIDE_EVENT_GROUP_C45D158B =
      "Logical backup control record inside an event group at offset %d in %s";
  public static final String EXCEPTION_LOGICAL_BACKUP_SEGMENT_ID_MISMATCH_ARG_9FE7E88A =
      "Logical backup segment ID mismatch: %s";
  public static final String EXCEPTION_LOGICAL_BACKUP_SEQUENCE_GAP_IN_ARG_D8698149 =
      "Logical backup sequence gap in %s";
  public static final String EXCEPTION_LOGICAL_BACKUP_HAS_NO_SEGMENTS_B48D5F15 =
      "Logical backup has no segments";
  public static final String EXCEPTION_INVALID_LOGICAL_BACKUP_SEGMENT_PATH_ARG_6485A845 =
      "Invalid logical backup segment path: %s";
  public static final String EXCEPTION_NO_LOGICAL_BACKUP_MANIFEST_FOUND_UNDER_ARG_4ACEA70E =
      "No logical backup manifest found under %s";
  public static final String EXCEPTION_DUPLICATE_LOGICAL_BACKUP_STREAM_ID_ARG_1BF0F6EE =
      "Duplicate logical backup stream ID: %s";
  public static final String EXCEPTION_LOGICAL_BACKUP_MANIFEST_HAS_NO_SEGMENTS_ARG_AFB5C138 =
      "Logical backup manifest has no segments: %s";
  public static final String EXCEPTION_UNLISTED_LOGICAL_BACKUP_SEGMENT_ARG_FF548C0B =
      "Unlisted logical backup segment %s";
  public static final String EXCEPTION_LOGICAL_BACKUP_SEGMENT_METADATA_MISMATCH_ARG_376FD0B3 =
      "Logical backup segment metadata mismatch: %s";
  public static final String EXCEPTION_LOGICAL_BACKUP_SEGMENT_IS_NOT_SEALED_ARG_937793FF =
      "Logical backup segment is not sealed: %s";
  public static final String
      EXCEPTION_LOGICAL_BACKUP_MANIFEST_COUNTERS_DO_NOT_MATCH_SEGMENT_CONTENTS_ARG_946818BB =
          "Logical backup manifest counters do not match segment contents: %s";
  public static final String
      EXCEPTION_LOGICAL_BACKUP_SEQUENCE_IS_NOT_CONTINUOUS_ACROSS_SEGMENTS_ARG_8547BC6E =
          "Logical backup sequence is not continuous across segments: %s";
  public static final String EXCEPTION_LOGICAL_BACKUP_MANIFEST_IS_INVALID_ARG_CB809CC7 =
      "Logical backup manifest is invalid: %s";
  public static final String EXCEPTION_LOGICAL_BACKUP_CONTAINS_SKIPPED_EVENTS_ARG_E69FD599 =
      "Logical backup contains skipped events: %s";

  public static final String
      EXCEPTION_LOGICAL_BACKUP_REQUEST_VERSION_ARG_IS_NOT_SUPPORTED_393457D7 =
          "Logical backup request version %d is not supported";
  public static final String EXCEPTION_LOGICAL_BACKUP_REQUEST_TYPE_ARG_IS_NOT_ALLOWED_7F1CDD38 =
      "Logical backup request type %d is not allowed";
  public static final String EXCEPTION_DUPLICATE_LOGICAL_BACKUP_EVENT_GROUP_ID_ARG_15B04C89 =
      "Duplicate logical backup event group ID: %s";
  public static final String
      EXCEPTION_SYMBOLIC_LINKS_ARE_NOT_ALLOWED_IN_LOGICAL_BACKUP_DIRECTORY_PATHS_ARG_7E428569 =
          "Symbolic links are not allowed in logical backup directory paths: %s";
  public static final String EXCEPTION_LOGICAL_BACKUP_DIRECTORY_IS_UNAVAILABLE_ARG_85F090AD =
      "Logical backup directory is unavailable: %s";

  private LogicalBackupMessages() {}
}
