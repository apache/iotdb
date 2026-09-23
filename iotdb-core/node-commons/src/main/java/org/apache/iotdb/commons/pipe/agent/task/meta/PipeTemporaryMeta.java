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

package org.apache.iotdb.commons.pipe.agent.task.meta;

public interface PipeTemporaryMeta {

  int TS_FILE_EPOCH_DEGRADED_STATUS_UNKNOWN = -1;
  int TS_FILE_EPOCH_DEGRADED_STATUS_FALSE = 0;
  int TS_FILE_EPOCH_DEGRADED_STATUS_TRUE = 1;

  static int encodeTsFileEpochDegradedStatus(final Boolean isDegraded) {
    if (isDegraded == null) {
      return TS_FILE_EPOCH_DEGRADED_STATUS_UNKNOWN;
    }
    return isDegraded ? TS_FILE_EPOCH_DEGRADED_STATUS_TRUE : TS_FILE_EPOCH_DEGRADED_STATUS_FALSE;
  }

  static Boolean decodeTsFileEpochDegradedStatus(final Integer status) {
    if (status == null) {
      return null;
    }
    switch (status) {
      case TS_FILE_EPOCH_DEGRADED_STATUS_FALSE:
        return false;
      case TS_FILE_EPOCH_DEGRADED_STATUS_TRUE:
        return true;
      case TS_FILE_EPOCH_DEGRADED_STATUS_UNKNOWN:
      default:
        return null;
    }
  }
}
