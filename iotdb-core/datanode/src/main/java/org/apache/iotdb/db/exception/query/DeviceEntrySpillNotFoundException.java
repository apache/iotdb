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

package org.apache.iotdb.db.exception.query;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IoTDBRuntimeException;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;

import static org.apache.iotdb.rpc.TSStatusCode.DEVICE_ENTRY_SPILL_NOT_FOUND;

/** Indicates that a spill segment was removed when its query was cleaned up. */
public class DeviceEntrySpillNotFoundException extends IoTDBRuntimeException {

  public DeviceEntrySpillNotFoundException(String path) {
    super(
        String.format(
            DataNodeQueryMessages
                .EXCEPTION_DEVICEENTRY_SPILL_SEGMENT_UNAVAILABLE_MAY_BE_DUE_TO_TIMEOUT_OR_KILL_ARG_B932D10D,
            path),
        DEVICE_ENTRY_SPILL_NOT_FOUND.getStatusCode(),
        true);
  }

  public DeviceEntrySpillNotFoundException(TSStatus status) {
    super(status);
  }
}
