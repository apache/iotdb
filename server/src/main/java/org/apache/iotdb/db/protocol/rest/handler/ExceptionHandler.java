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

import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.DatabaseNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.exception.sql.StatementAnalyzeException;
import org.apache.iotdb.db.protocol.rest.model.ExecutionStatus;
import org.apache.iotdb.rpc.TSStatusCode;

import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response.Status;

import java.io.IOException;

public class ExceptionHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExceptionHandler.class);

  private ExceptionHandler() {}

  public static ExecutionStatus tryCatchException(Exception e) {
    ExecutionStatus responseResult = new ExecutionStatus();
    if (e instanceof QueryProcessException) {
      responseResult.setMessage(e.getMessage());
      responseResult.setCode(((QueryProcessException) e).getErrorCode());
    } else if (e instanceof DatabaseNotSetException) {
      responseResult.setMessage(e.getMessage());
      responseResult.setCode(((DatabaseNotSetException) e).getErrorCode());
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
    } else if (e instanceof ParseCancellationException) {
      responseResult.setMessage(e.getMessage());
      responseResult.setCode(TSStatusCode.SQL_PARSE_ERROR.getStatusCode());
    } else if (e instanceof StatementAnalyzeException) {
      responseResult.setMessage(e.getMessage());
      responseResult.setCode(TSStatusCode.METADATA_ERROR.getStatusCode());
    } else if (e instanceof SemanticException) {
      responseResult.setMessage(e.getMessage());
      responseResult.setCode(TSStatusCode.SEMANTIC_ERROR.getStatusCode());
    } else if (!(e instanceof IOException) && !(e instanceof RuntimeException)) {
      responseResult.setMessage(e.getMessage());
      responseResult.setCode(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
    } else {
      responseResult.setMessage(e.getMessage());
      responseResult.setCode(TSStatusCode.INTERNAL_SERVER_ERROR.getStatusCode());
    }
    LOGGER.warn(e.getMessage(), e);
    return responseResult;
  }
}
