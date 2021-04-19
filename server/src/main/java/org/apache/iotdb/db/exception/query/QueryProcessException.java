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

import org.apache.iotdb.db.exception.IoTDBException;
import org.apache.iotdb.rpc.TSStatusCode;

public class QueryProcessException extends IoTDBException {

  private static final long serialVersionUID = -683191083844850054L;

  public QueryProcessException(String message) {
    super(message, TSStatusCode.QUERY_PROCESS_ERROR.getStatusCode());
  }

  public QueryProcessException(String message, boolean isUserException) {
    super(message, TSStatusCode.QUERY_PROCESS_ERROR.getStatusCode(), isUserException);
  }

  public QueryProcessException(String message, int errorCode) {
    super(message, errorCode);
  }

  public QueryProcessException(IoTDBException e) {
    super(e, e.getErrorCode(), e.isUserException());
  }

  public QueryProcessException(Throwable cause, int errorCode) {
    super(cause, errorCode);
  }
}
