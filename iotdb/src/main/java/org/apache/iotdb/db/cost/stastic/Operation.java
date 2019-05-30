/**
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
package org.apache.iotdb.db.cost.stastic;

public enum Operation {
  EXECUTE_BATCH_SQL("EXECUTE_BATCH_SQL"),
  PARSE_SQL_TO_PHYSICAL_PLAN("1 PARSE_SQL_TO_PHYSICAL_PLAN"),
  GENERATE_AST_NODE("1.1 GENERATE_AST_NODE"),
  GENERATE_PHYSICAL_PLAN("1.2 GENERATE_PHYSICAL_PLAN"),
  EXECUTE_PHYSICAL_PLAN("2 EXECUTE_PHYSICAL_PLAN"),
  CHECK_AUTHORIZATION("2.1 CHECK_AUTHORIZATION"),
  EXECUTE_NON_QUERY("2.2 EXECUTE_NON_QUERY"),
  CONSTRUCT_TSRECORD("2.2.1 CONSTRUCT_TSRECORD"),
  GET_FILENODE_PROCESSOR("2.2.2 GET_FILENODE_PROCESSOR(ADD LOCK)"),
  INSERT_BUFFER_WRITE_OR_OVERFLOW("2.2.3 INSERT_BUFFER_WRITE_OR_OVERFLOW"),
  GET_BUFFER_WRITE_PROFESSOR("2.2.3.1 GET_BUFFER_WRITE_PROFESSOR"),
  WRITE_WAL("2.2.3.2 WRITE_WAL"),
  WRITE_MEM_TABLE("2.2.3.3 WRITE_MEM_TABLE"),
  CONSTRUCT_JDBC_RESULT("2.3 CONSTRUCT_JDBC_RESULT"),;

  public String getName() {
    return name;
  }

  String name;

  Operation(String name) {
    this.name = name;
  }

}
