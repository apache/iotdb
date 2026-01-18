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

package org.apache.iotdb.db.queryengine.plan.execution.config.session;

import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.PreparedStatementInfo;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement;

/**
 * Helper class for managing prepared statement registration and unregistration. Provides common
 * logic shared between RPC methods and ConfigTask implementations.
 */
public class PreparedStatementHelper {

  private PreparedStatementHelper() {
    // Utility class
  }

  /**
   * Registers a prepared statement in the session.
   *
   * <p>This method performs the following operations:
   *
   * <ol>
   *   <li>Checks if a prepared statement with the same name already exists
   *   <li>Calculates memory size of the AST
   *   <li>Allocates memory from PreparedStatementMemoryManager
   *   <li>Creates and stores PreparedStatementInfo in the session
   * </ol>
   *
   * @param session the client session
   * @param statementName the name of the prepared statement
   * @param sql the parsed SQL statement (AST)
   * @return the created PreparedStatementInfo
   * @throws SemanticException if a prepared statement with the same name already exists
   */
  public static PreparedStatementInfo register(
      IClientSession session, String statementName, Statement sql) {
    // Check if prepared statement with the same name already exists
    if (session.getPreparedStatement(statementName) != null) {
      throw new SemanticException(
          String.format("Prepared statement '%s' already exists", statementName));
    }

    // Calculate memory size of the AST
    long memorySizeInBytes = sql == null ? 0L : sql.ramBytesUsed();

    // Allocate memory from PreparedStatementMemoryManager
    PreparedStatementMemoryManager.getInstance().allocate(statementName, memorySizeInBytes);

    // Create and store PreparedStatementInfo
    PreparedStatementInfo info = new PreparedStatementInfo(statementName, sql, memorySizeInBytes);
    session.addPreparedStatement(statementName, info);

    return info;
  }

  /**
   * Unregisters a prepared statement from the session.
   *
   * <p>This method performs the following operations:
   *
   * <ol>
   *   <li>Removes the prepared statement from the session
   *   <li>Releases the allocated memory
   * </ol>
   *
   * @param session the client session
   * @param statementName the name of the prepared statement to remove
   * @return the removed PreparedStatementInfo
   * @throws SemanticException if the prepared statement does not exist
   */
  public static PreparedStatementInfo unregister(IClientSession session, String statementName) {
    // Remove the prepared statement
    PreparedStatementInfo removedInfo = session.removePreparedStatement(statementName);
    if (removedInfo == null) {
      throw new SemanticException(
          String.format("Prepared statement '%s' does not exist", statementName));
    }

    // Release the allocated memory
    PreparedStatementMemoryManager.getInstance().release(removedInfo.getMemorySizeInBytes());

    return removedInfo;
  }
}
