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

package org.apache.iotdb.pipe.api.exception;

import org.apache.iotdb.pipe.api.i18n.PipeApiMessages;

public class PipeConnectionException extends PipeException {

  public static final String CONNECTION_ERROR_FORMATTER =
      PipeApiMessages
          .EXCEPTION_ERROR_OCCURRED_WHILE_CONNECTING_TO_RECEIVER_ARG_ARG_PLEASE_CHECK_NETWORK_CONNECTIVITY_OR_SSL_CONFIGURATIONS_WHEN_ENABLE_SSL_TRANSMISSION_5C0C2E01;

  public PipeConnectionException(final String message) {
    super(message);
  }

  public PipeConnectionException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
