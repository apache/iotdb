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

package org.apache.iotdb.commons.exception;

import org.apache.iotdb.rpc.TSStatusCode;

public class UncheckedStartupException extends IoTDBRuntimeException {

  private static final long serialVersionUID = -8591716406230730147L;

  public UncheckedStartupException(String name, String message) {
    super(
        String.format("Failed to start [%s], because [%s]", name, message),
        TSStatusCode.START_UP_ERROR.getStatusCode());
  }

  public UncheckedStartupException(Throwable cause) {
    super(cause.getMessage(), TSStatusCode.START_UP_ERROR.getStatusCode());
  }

  public UncheckedStartupException(String message) {
    super(message, TSStatusCode.START_UP_ERROR.getStatusCode());
  }
}
