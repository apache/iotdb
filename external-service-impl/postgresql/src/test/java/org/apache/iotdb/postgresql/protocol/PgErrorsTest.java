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

package org.apache.iotdb.postgresql.protocol;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class PgErrorsTest {

  private static String state(TSStatusCode code, String message) {
    return PgErrors.sqlState(code.getStatusCode(), message);
  }

  @Test
  public void codesMapToTheContractTable() {
    assertEquals("42601", state(TSStatusCode.SQL_PARSE_ERROR, "line 1:8: mismatched input"));
    assertEquals("42P01", state(TSStatusCode.TABLE_NOT_EXISTS, "x"));
    assertEquals("42703", state(TSStatusCode.COLUMN_NOT_EXISTS, "x"));
    assertEquals("3F000", state(TSStatusCode.DATABASE_NOT_EXIST, "x"));
    assertEquals("42501", state(TSStatusCode.NO_PERMISSION, "x"));
    assertEquals("57014", state(TSStatusCode.QUERY_WAS_KILLED, "x"));
    assertEquals("57014", state(TSStatusCode.QUERY_TIMEOUT, "x"));
    assertEquals("53200", state(TSStatusCode.MPP_MEMORY_NOT_ENOUGH, "x"));
    assertEquals("0A000", state(TSStatusCode.UNSUPPORTED_OPERATION, "x"));
    assertEquals("22012", state(TSStatusCode.DIVISION_BY_ZERO, "x"));
    assertEquals("XX000", state(TSStatusCode.INTERNAL_SERVER_ERROR, "x"));
  }

  @Test
  public void semanticErrorsAreReadFromTheMessage() {
    assertEquals("42P01", state(TSStatusCode.SEMANTIC_ERROR, "Table 'db.t' does not exist."));
    assertEquals("42703", state(TSStatusCode.SEMANTIC_ERROR, "Column 'speed' cannot be resolved"));
    assertEquals("3F000", state(TSStatusCode.SEMANTIC_ERROR, "Database db2 does not exist"));
    assertEquals("42000", state(TSStatusCode.SEMANTIC_ERROR, "GROUP BY clause is missing"));
  }

  @Test
  public void engineErrorsAreNotFatalAndKeepTheMessage() {
    PgException error =
        PgErrors.of(new TSStatus(TSStatusCode.TABLE_NOT_EXISTS.getStatusCode()).setMessage("gone"));
    assertFalse(error.isFatal());
    assertEquals("42P01", error.sqlState());
    assertEquals("gone", error.getMessage());
  }
}
