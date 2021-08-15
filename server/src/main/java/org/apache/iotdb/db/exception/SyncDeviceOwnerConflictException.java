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
package org.apache.iotdb.db.exception;

import org.apache.iotdb.rpc.TSStatusCode;

public class SyncDeviceOwnerConflictException extends IoTDBException {

  private static final long serialVersionUID = -5037926672199248044L;

  public SyncDeviceOwnerConflictException(String message) {
    super(message, TSStatusCode.SYNC_DEVICE_OWNER_CONFLICT_ERROR.getStatusCode());
  }

  public SyncDeviceOwnerConflictException(
      String device, String correctOwner, String conflictOwner) {
    super(
        String.format(
            "Device: [%s], correct owner: [%s], conflict owner: [%s]",
            device, correctOwner, conflictOwner),
        TSStatusCode.SYNC_DEVICE_OWNER_CONFLICT_ERROR.getStatusCode());
  }
}
