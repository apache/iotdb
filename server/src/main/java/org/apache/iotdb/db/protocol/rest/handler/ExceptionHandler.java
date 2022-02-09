/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.protocol.rest.handler;

import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.exception.IoTDBException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.protocol.rest.model.ExecutionStatus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response.Status;

public class ExceptionHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExceptionHandler.class);

  private ExceptionHandler() {}

  public static ExecutionStatus tryCatchException(Exception e) {
    ExecutionStatus responseResult = new ExecutionStatus();
    if (e instanceof QueryProcessException) {
      responseResult.setMessage(e.getMessage());
      responseResult.setCode(((QueryProcessException) e).getErrorCode());
    } else if (e instanceof StorageGroupNotSetException) {
      responseResult.setMessage(e.getMessage());
      responseResult.setCode(((StorageGroupNotSetException) e).getErrorCode());
    } else if (e instanceof StorageEngineException) {
      responseResult.setMessage(e.getMessage());
      responseResult.setCode(((StorageEngineException) e).getErrorCode());
    } else if (e instanceof AuthException) {
      responseResult.setMessage(e.getMessage());
      responseResult.setCode(Status.BAD_REQUEST.getStatusCode());
    } else if (e instanceof IllegalPathException) {
      responseResult.setMessage(e.getMessage());
      responseResult.setCode(((IllegalPathException) e).getErrorCode());
    } else if (e instanceof MetadataException) {
      responseResult.setMessage(e.getMessage());
      responseResult.setCode(((MetadataException) e).getErrorCode());
    } else if (e instanceof IoTDBException) {
      responseResult.setMessage(e.getMessage());
      responseResult.setCode(((IoTDBException) e).getErrorCode());
    } else {
      responseResult.setMessage(e.getMessage());
      responseResult.setCode(Status.INTERNAL_SERVER_ERROR.getStatusCode());
    }
    LOGGER.warn(e.getMessage(), e);
    return responseResult;
  }
}
