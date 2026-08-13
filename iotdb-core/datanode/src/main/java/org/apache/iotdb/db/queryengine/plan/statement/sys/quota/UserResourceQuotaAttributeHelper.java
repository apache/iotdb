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

package org.apache.iotdb.db.queryengine.plan.statement.sys.quota;

import org.apache.iotdb.common.rpc.thrift.TTimedQuota;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;

/**
 * Shared attribute parsing for {@code SET USER QUOTA} (tree and table model).
 *
 * <p>Attribute keys are underscore-separated and case-insensitive. General form:
 *
 * <pre>
 *   {read|write}_{cpu|memory}_{min|max}       e.g. read_cpu_max, write_memory_min
 *   {read|write}_temp_disk_{min|max}          e.g. write_temp_disk_max
 *   {read|write}_disk_io_max                  e.g. read_disk_io_max (rate only; no min)
 * </pre>
 *
 * Values are positive longs: CPU as slot count; memory / temp_disk as bytes; disk_io as bytes/sec.
 * Unset min/max are passed as {@code -1} and merged later in {@link
 * SetUserResourceQuotaStatement#putRange}.
 */
public final class UserResourceQuotaAttributeHelper {

  private UserResourceQuotaAttributeHelper() {}

  /**
   * Parse one {@code key=value} pair and write it into {@code statement}.
   *
   * <p>Key layout after {@code split("_")}: {@code parts[0]} = operation side (READ/WRITE); middle
   * token(s) = resource; last token = bound ({@code min}/{@code max}). Multi-token resources
   * ({@code temp_disk}, {@code disk_io}) are matched before single-token ones.
   */
  public static void applyAttribute(
      SetUserResourceQuotaStatement statement, String key, String value) {
    String[] parts = key.toLowerCase().split("_");
    if (parts.length < 3) {
      throw new SemanticException(
          String.format(
              DataNodeQueryMessages.EXCEPTION_INVALID_USER_QUOTA_ATTRIBUTE_ARG_D6CC7292, key));
    }
    // parts[0]: read | write
    SetUserResourceQuotaStatement.OperationSide side;
    try {
      side = SetUserResourceQuotaStatement.OperationSide.valueOf(parts[0].toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new SemanticException(
          String.format(
              DataNodeQueryMessages.EXCEPTION_INVALID_USER_QUOTA_ATTRIBUTE_ARG_D6CC7292, key));
    }
    String bound = parts[parts.length - 1];
    // Special case: {side}_disk_io_max → throttle rate (bytes/sec), only max allowed.
    if ("disk".equals(parts[1]) && "io".equals(parts[2])) {
      if (!"max".equals(bound) || parts.length != 4) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages.EXCEPTION_INVALID_USER_QUOTA_ATTRIBUTE_ARG_D6CC7292, key));
      }
      // Fixed unit: bytes/sec as positive long.
      long bytesPerSec =
          parsePositiveLong(
              value,
              DataNodeQueryMessages
                  .EXCEPTION_INVALID_USER_QUOTA_DISK_IO_VALUE_ARG_EXPECTED_POSITIVE_LONG_BYTES_SEC_C140D430);
      statement.putDiskIo(side, new TTimedQuota(IoTDBConstant.SEC, bytesPerSec));
      return;
    }
    // Capacity-style resources: cpu / memory / temp_disk with optional min/max range.
    SetUserResourceQuotaStatement.ResourceSide resource;
    // temp_disk spans two tokens; others are a single token matching ResourceSide.
    if ("temp".equals(parts[1]) && "disk".equals(parts[2]) && parts.length >= 4) {
      resource = SetUserResourceQuotaStatement.ResourceSide.TEMP_DISK;
    } else {
      try {
        resource = SetUserResourceQuotaStatement.ResourceSide.valueOf(parts[1].toUpperCase());
      } catch (IllegalArgumentException e) {
        throw new SemanticException(
            String.format(
                DataNodeQueryMessages.EXCEPTION_INVALID_USER_QUOTA_ATTRIBUTE_ARG_D6CC7292, key));
      }
    }
    if (!"min".equals(bound) && !"max".equals(bound)) {
      throw new SemanticException(
          String.format(
              DataNodeQueryMessages.EXCEPTION_INVALID_USER_QUOTA_ATTRIBUTE_ARG_D6CC7292, key));
    }
    long parsed;
    if (resource == SetUserResourceQuotaStatement.ResourceSide.CPU) {
      // Fixed unit: positive long slot count.
      parsed =
          parsePositiveLong(
              value,
              DataNodeQueryMessages
                  .EXCEPTION_INVALID_USER_QUOTA_CPU_VALUE_ARG_EXPECTED_POSITIVE_LONG_ECF3E7E1);
    } else if (resource == SetUserResourceQuotaStatement.ResourceSide.MEMORY) {
      // Fixed unit: bytes as positive long (same as temp_disk).
      parsed =
          parsePositiveLong(
              value,
              DataNodeQueryMessages
                  .EXCEPTION_INVALID_USER_QUOTA_MEMORY_VALUE_ARG_EXPECTED_POSITIVE_LONG_BYTES_E68B3BF1);
    } else if (resource == SetUserResourceQuotaStatement.ResourceSide.TEMP_DISK) {
      // Fixed unit: bytes as positive long.
      parsed =
          parsePositiveLong(
              value,
              DataNodeQueryMessages
                  .EXCEPTION_INVALID_USER_QUOTA_TEMP_DISK_VALUE_ARG_EXPECTED_POSITIVE_LONG_BYTES_B306C6BF);
    } else {
      throw new SemanticException(
          String.format(
              DataNodeQueryMessages.EXCEPTION_INVALID_USER_QUOTA_ATTRIBUTE_ARG_D6CC7292, key));
    }
    // Only one bound is set per attribute; the other stays -1 and is merged in putRange.
    long min = -1;
    long max = -1;
    if ("min".equals(bound)) {
      min = parsed;
    } else {
      max = parsed;
    }
    statement.putRange(side, resource, min, max);
  }

  /** Parse {@code value} as a strictly positive long, or throw with {@code errorTemplate}. */
  private static long parsePositiveLong(String value, String errorTemplate) {
    long parsed;
    try {
      parsed = Long.parseLong(value);
    } catch (NumberFormatException e) {
      throw new SemanticException(String.format(errorTemplate, value));
    }
    if (parsed <= 0) {
      throw new SemanticException(String.format(errorTemplate, value));
    }
    return parsed;
  }
}
