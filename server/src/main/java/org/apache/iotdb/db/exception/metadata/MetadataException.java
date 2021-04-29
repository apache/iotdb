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
package org.apache.iotdb.db.exception.metadata;

import org.apache.iotdb.db.exception.IoTDBException;
import org.apache.iotdb.rpc.TSStatusCode;

/**
 * If query metadata constructs schema but passes illegal parameters to EncodingConvertor or
 * DataTypeConverter,this exception will be threw.
 */
public class MetadataException extends IoTDBException {

  private static final long serialVersionUID = 3415275599091623570L;

  public MetadataException(Throwable cause) {
    super(cause, TSStatusCode.METADATA_ERROR.getStatusCode());
  }

  public MetadataException(Throwable cause, int errorCode) {
    super(cause, errorCode);
  }

  public MetadataException(String msg) {
    super(msg, TSStatusCode.METADATA_ERROR.getStatusCode());
  }

  public MetadataException(String msg, boolean isUserException) {
    super(msg, TSStatusCode.METADATA_ERROR.getStatusCode(), isUserException);
  }

  public MetadataException(String message, Throwable cause) {
    super(message + cause.getMessage(), TSStatusCode.METADATA_ERROR.getStatusCode());
  }

  public MetadataException(IoTDBException exception) {
    super(exception.getMessage(), exception.getErrorCode());
  }

  public MetadataException(String message, int errorCode) {
    super(message, errorCode);
  }

  public MetadataException(String message, int errorCode, boolean isUserException) {
    super(message, errorCode, isUserException);
  }
}
