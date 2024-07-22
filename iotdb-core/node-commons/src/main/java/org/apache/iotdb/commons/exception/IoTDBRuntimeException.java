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
 *
 */

package org.apache.iotdb.commons.exception;

public class IoTDBRuntimeException extends RuntimeException {
  protected int errorCode;

  /**
   * This kind of exception is caused by users' wrong sql, and there is no need for server to print
   * the full stack of the exception
   */
  protected boolean isUserException = false;

  public IoTDBRuntimeException(String message, int errorCode) {
    super(message);
    this.errorCode = errorCode;
  }

  public IoTDBRuntimeException(String message, int errorCode, boolean isUserException) {
    super(message);
    this.errorCode = errorCode;
    this.isUserException = isUserException;
  }

  public IoTDBRuntimeException(String message, Throwable cause, int errorCode) {
    super(message, cause);
    this.errorCode = errorCode;
  }

  public IoTDBRuntimeException(Throwable cause, int errorCode) {
    super(cause);
    this.errorCode = errorCode;
  }

  public IoTDBRuntimeException(Throwable cause, int errorCode, boolean isUserException) {
    super(cause);
    this.errorCode = errorCode;
    this.isUserException = isUserException;
  }

  public boolean isUserException() {
    return isUserException;
  }

  public int getErrorCode() {
    return errorCode;
  }
}
