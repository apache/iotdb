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
package org.apache.iotdb.db.utils;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.db.conf.OperationType;
import org.apache.iotdb.db.exception.BatchProcessException;
import org.apache.iotdb.db.exception.QueryInBatchStatementException;
import org.apache.iotdb.db.exception.StorageGroupNotReadyException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.query.QueryTimeoutRuntimeException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.exception.TsFileRuntimeException;

import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

public class ErrorHandlingUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(ErrorHandlingUtils.class);
  private static final Logger DETAILED_FAILURE_QUERY_TRACE_LOGGER =
      LoggerFactory.getLogger("DETAILED_FAILURE_QUERY_TRACE");

  private static final String INFO_PARSING_SQL_ERROR =
      "Error occurred while parsing SQL to physical plan: ";
  private static final String INFO_CHECK_METADATA_ERROR = "Check metadata error: ";
  private static final String INFO_QUERY_PROCESS_ERROR = "Error occurred in query process: ";
  private static final String INFO_NOT_ALLOWED_IN_BATCH_ERROR =
      "The query statement is not allowed in batch: ";

  public static TSStatus onNPEOrUnexpectedException(
      Exception e, String operation, TSStatusCode statusCode) {
    String message = String.format("[%s] Exception occurred: %s failed. ", statusCode, operation);
    if (e instanceof IOException || e instanceof NullPointerException) {
      LOGGER.error("Status code: " + statusCode + ", operation: " + operation + " failed", e);
    } else {
      LOGGER.warn("Status code: " + statusCode + ", operation: " + operation + " failed", e);
    }
    return RpcUtils.getStatus(statusCode, message + e.getMessage());
  }

  public static TSStatus onNPEOrUnexpectedException(
      Exception e, OperationType operation, TSStatusCode statusCode) {
    return onNPEOrUnexpectedException(e, operation.getName(), statusCode);
  }

  public static Throwable getRootCause(Throwable e) {
    while (e.getCause() != null) {
      e = e.getCause();
    }
    return e;
  }

  public static TSStatus onQueryException(Exception e, String operation, TSStatusCode statusCode) {
    TSStatus status = tryCatchQueryException(e);
    if (status != null) {
      // ignore logging sg not ready exception
      if (status.getCode() != TSStatusCode.STORAGE_ENGINE_NOT_READY.getStatusCode()) {
        String message =
            String.format(
                "Status code: %s, Query Statement: %s failed", status.getCode(), operation);
        if (status.getCode() == TSStatusCode.SQL_PARSE_ERROR.getStatusCode()) {
          LOGGER.warn(message);
        } else {
          LOGGER.warn(message, e);
        }
      }
      return status;
    } else {
      return onNPEOrUnexpectedException(e, operation, statusCode);
    }
  }

  public static TSStatus onQueryException(Exception e, String operation) {
    return onQueryException(e, operation, TSStatusCode.INTERNAL_SERVER_ERROR);
  }

  public static TSStatus onQueryException(Exception e, OperationType operation) {
    return onQueryException(e, operation.getName());
  }

  public static TSStatus tryCatchQueryException(Exception e) {
    Throwable rootCause = getRootCause(e);
    // ignore logging sg not ready exception
    if (rootCause instanceof StorageGroupNotReadyException) {
      return RpcUtils.getStatus(TSStatusCode.STORAGE_ENGINE_NOT_READY, rootCause.getMessage());
    }

    Throwable t = e instanceof ExecutionException ? e.getCause() : e;
    if (t instanceof QueryTimeoutRuntimeException) {
      return RpcUtils.getStatus(TSStatusCode.INTERNAL_REQUEST_TIME_OUT, rootCause.getMessage());
    } else if (t instanceof ParseCancellationException) {
      return RpcUtils.getStatus(
          TSStatusCode.SQL_PARSE_ERROR, INFO_PARSING_SQL_ERROR + rootCause.getMessage());
    } else if (t instanceof QueryProcessException) {
      return RpcUtils.getStatus(
          TSStatusCode.QUERY_PROCESS_ERROR, INFO_QUERY_PROCESS_ERROR + rootCause.getMessage());
    } else if (t instanceof QueryInBatchStatementException) {
      return RpcUtils.getStatus(
          TSStatusCode.QUERY_NOT_ALLOWED, INFO_NOT_ALLOWED_IN_BATCH_ERROR + rootCause.getMessage());
    } else if (t instanceof IoTDBException) {
      return RpcUtils.getStatus(((IoTDBException) t).getErrorCode(), rootCause.getMessage());
    } else if (t instanceof TsFileRuntimeException) {
      return RpcUtils.getStatus(TSStatusCode.TSFILE_PROCESSOR_ERROR, rootCause.getMessage());
    } else if (t instanceof SemanticException) {
      return RpcUtils.getStatus(TSStatusCode.SEMANTIC_ERROR, rootCause.getMessage());
    }

    if (t instanceof RuntimeException && rootCause instanceof IoTDBException) {
      return RpcUtils.getStatus(
          ((IoTDBException) rootCause).getErrorCode(), rootCause.getMessage());
    }

    return null;
  }

  public static TSStatus onNonQueryException(Exception e, String operation) {
    TSStatus status = tryCatchNonQueryException(e);
    return status != null
        ? status
        : onNPEOrUnexpectedException(e, operation, TSStatusCode.INTERNAL_SERVER_ERROR);
  }

  public static TSStatus onNonQueryException(Exception e, OperationType operation) {
    return onNonQueryException(e, operation.getName());
  }

  public static TSStatus tryCatchNonQueryException(Exception e) {
    String message = "Exception occurred while processing non-query. ";
    if (e instanceof BatchProcessException) {
      BatchProcessException batchException = (BatchProcessException) e;
      // ignore logging sg not ready exception
      for (TSStatus status : batchException.getFailingStatus()) {
        if (status.getCode() == TSStatusCode.STORAGE_ENGINE_NOT_READY.getStatusCode()) {
          return RpcUtils.getStatus(Arrays.asList(batchException.getFailingStatus()));
        }
      }
      LOGGER.warn(message, e);
      return RpcUtils.getStatus(Arrays.asList(batchException.getFailingStatus()));
    } else if (e instanceof IoTDBException) {
      Throwable rootCause = getRootCause(e);
      // ignore logging sg not ready exception
      if (!(rootCause instanceof StorageGroupNotReadyException)) {
        LOGGER.warn(message, e);
      }
      return RpcUtils.getStatus(((IoTDBException) e).getErrorCode(), rootCause.getMessage());
    }
    return null;
  }

  public static TSStatus onIoTDBException(Exception e, String operation, int errorCode) {
    TSStatusCode statusCode = TSStatusCode.representOf(errorCode);
    String message =
        String.format(
            "[%s] Exception occurred: %s failed. %s", statusCode, operation, e.getMessage());
    LOGGER.warn("Status code: " + statusCode + ", operation: " + operation + " failed", e);
    return RpcUtils.getStatus(errorCode, message);
  }

  public static TSStatus onIoTDBException(Exception e, OperationType operation, int errorCode) {
    return onIoTDBException(e, operation.getName(), errorCode);
  }
}
