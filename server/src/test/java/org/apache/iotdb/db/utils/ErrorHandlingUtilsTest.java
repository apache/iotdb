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
import org.apache.iotdb.db.conf.OperationType;
import org.apache.iotdb.db.exception.StorageGroupNotReadyException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Test;

import java.io.IOException;

import static org.apache.iotdb.db.utils.ErrorHandlingUtils.onIoTDBException;
import static org.apache.iotdb.db.utils.ErrorHandlingUtils.onNonQueryException;
import static org.apache.iotdb.db.utils.ErrorHandlingUtils.onNpeOrUnexpectedException;
import static org.apache.iotdb.db.utils.ErrorHandlingUtils.onQueryException;
import static org.junit.Assert.assertEquals;

public class ErrorHandlingUtilsTest {

  @Test
  public void onNpeOrUnexpectedExceptionTest() {
    TSStatus status =
        onNpeOrUnexpectedException(
            new IOException("test-IOException"),
            OperationType.EXECUTE_STATEMENT,
            TSStatusCode.EXECUTE_STATEMENT_ERROR);
    assertEquals(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(), status.getCode());

    status =
        onNpeOrUnexpectedException(
            new NullPointerException("test-NullPointerException"),
            OperationType.EXECUTE_STATEMENT,
            TSStatusCode.EXECUTE_STATEMENT_ERROR);
    assertEquals(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(), status.getCode());

    status =
        onNpeOrUnexpectedException(
            new RuntimeException("test-RuntimeException"),
            OperationType.EXECUTE_STATEMENT,
            TSStatusCode.EXECUTE_STATEMENT_ERROR);
    assertEquals(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(), status.getCode());
  }

  @Test
  public void onQueryExceptionTest() {
    TSStatus status =
        onQueryException(
            new StorageGroupNotReadyException("test-StorageGroupNotReadyException", 0),
            OperationType.EXECUTE_STATEMENT);
    assertEquals(TSStatusCode.STORAGE_ENGINE_NOT_READY.getStatusCode(), status.getCode());

    status =
        onQueryException(
            new SemanticException("test-SemanticException"), OperationType.EXECUTE_STATEMENT);
    assertEquals(TSStatusCode.SEMANTIC_ERROR.getStatusCode(), status.getCode());
  }

  @Test
  public void onNonQueryExceptionTest() {
    TSStatus status =
        onNonQueryException(
            new PathNotExistException("test-PathNotExistException"),
            OperationType.EXECUTE_STATEMENT);
    assertEquals(TSStatusCode.PATH_NOT_EXIST.getStatusCode(), status.getCode());
  }

  @Test
  public void onIoTDBExceptionTest() {
    TSStatus status =
        onIoTDBException(
            new QueryProcessException("test-QueryProcessException"),
            OperationType.EXECUTE_STATEMENT,
            TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
    assertEquals(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(), status.getCode());
  }
}
