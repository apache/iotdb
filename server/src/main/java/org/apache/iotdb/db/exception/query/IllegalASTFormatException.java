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

import org.apache.iotdb.rpc.TSStatusCode;

/**
 * This exception is thrown while meeting error in parsing ast tree to generate logical operator.
 */
public class IllegalASTFormatException extends QueryProcessException {

  private static final long serialVersionUID = -8987915911329315588L;

  /**
   * Thrown for detailed SQL statement
   *
   * @param sqlStatement SQL statement
   * @param message detailed error message
   */
  public IllegalASTFormatException(String sqlStatement, String message) {
    super(String.format(
        "Parsing error, statement [%s] failed when parsing AST tree to generate logical operator. Detailed information: [%s]",
        sqlStatement, message));
    errorCode = TSStatusCode.AST_FORMAT_ERROR.getStatusCode();
  }

  /**
   * Thrown for auth command, for example "grant author" or "update password"
   *
   * @param authCommand auth command
   */
  public IllegalASTFormatException(String authCommand) {
    super(String.format(
        "Parsing error, [%s] command failed when parsing AST tree to generate logical operator. Please check you SQL statement",
        authCommand));
    errorCode = TSStatusCode.AST_FORMAT_ERROR.getStatusCode();
  }

  /**
   * Thrown for other commands, for example "data load"
   *
   * @param command command type
   * @param message exception message
   * @param detailedMessage exception detailed message
   */
  public IllegalASTFormatException(String command, String message, String detailedMessage) {
    super(String.format(
        "Parsing error, [%s] command failed when parsing AST tree to generate logical operator. Detailed information: [%s]",
        command, message + detailedMessage));
    errorCode = TSStatusCode.AST_FORMAT_ERROR.getStatusCode();
  }
}
